"""A bounded local collection pass over the selected games; no Kafka required.

Sources (default all):
  store      GetItems in batches of 200: tags+weights, review summary, release/early access (1 + n/200 requests)
  metadata   appdetails: price, genres, categories, developers, platforms, DLC, achievements ...
  histogram  daily up/down review counts for the last ~30 days
  ccu        current players (one snapshot per run; schedule runs to build a time series)
  reviews    incremental: new reviews since the last run, then history backfill to --review-days
Re-running never re-emits a review already collected under the same --state-dir.
"""
import argparse
import logging
import os
import signal
import time
from pathlib import Path

from crawlers import steam_app_details_crawler as metadata
from crawlers import steam_ccu_crawler as ccu
from crawlers import steam_review_histogram_crawler as histogram
from crawlers import steam_reviews_crawler as reviews
from crawlers.config import (TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT, OUTPUT_DIR, STATE_DIR,
                             CRAWLER_RATE_LIMIT_DELAY, CRAWLER_REVIEW_BATCHES, CRAWLER_REVIEW_DAYS,
                             CRAWLER_WORKER_ID, CRAWLER_TOTAL_WORKERS, KAFKA_TOPIC_META,
                             CRAWLER_FLUSH_MAX_RECORDS, CRAWLER_FLUSH_INTERVAL_SEC)
from crawlers.common import steam_store
from crawlers.common.jsonl_producer import JsonlProducer
from crawlers.common.files import write_json
from crawlers.common.rate_limiter import RateLimiter, RequestBudgetExceeded
from crawlers.common.review_state import ReviewStateStore
from crawlers.common.targets import load_target_apps, is_assigned_to_worker

SOURCES = ['store', 'metadata', 'histogram', 'ccu', 'reviews']
PER_GAME = ['metadata', 'histogram', 'ccu', 'reviews']


def collect_store(apps, producer, limiter):
    """Batched store snapshot for all assigned games. Returns the appids Steam did not return."""
    tag_names = steam_store.fetch_tag_names(limiter)
    items = steam_store.fetch_store_items(apps, limiter)
    now = time.time()
    for appid in apps:
        if appid in items and items[appid].get('success') == 1:
            # Distinct key per record type: steam.games.meta is log-compacted by key
            producer.send(KAFKA_TOPIC_META, f'{appid}:store', steam_store.parse_store_item(items[appid], tag_names, now))
    if hasattr(producer, 'flush_all'):
        producer.flush_all()
    else:
        producer.flush()
    return [a for a in apps if a not in items or items[a].get('success') != 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--targets', default=TARGET_APP_LIST_PATH)
    parser.add_argument('--limit', type=int, default=CRAWLER_TARGET_LIMIT)
    parser.add_argument('--sources', nargs='+', choices=SOURCES, default=SOURCES)
    parser.add_argument('--output-dir', default=OUTPUT_DIR)
    parser.add_argument('--state-dir', default=STATE_DIR)
    parser.add_argument('--interval', type=float, default=CRAWLER_RATE_LIMIT_DELAY)
    parser.add_argument('--max-requests', type=int, default=3000)
    parser.add_argument('--review-pages', dest='review_batches', type=int,
                        default=CRAWLER_REVIEW_BATCHES, help='Review pages (100 reviews) per game per run')
    parser.add_argument('--review-days', type=int, default=CRAWLER_REVIEW_DAYS,
                        help='Backfill reviews created within this many days (0 = full history)')
    parser.add_argument('--max-consecutive-errors', type=int, default=5)
    parser.add_argument('--worker-id', type=int, default=CRAWLER_WORKER_ID)
    parser.add_argument('--workers', type=int, default=CRAWLER_TOTAL_WORKERS)
    parser.add_argument('--start-index', type=int, default=0,
                        help='Skip this many games assigned to the worker; use next_game_index from an interrupted pass')
    parser.add_argument('--flush-max-records', type=int,
                        default=CRAWLER_FLUSH_MAX_RECORDS,
                        help='Max records in memory buffer before flushing to jsonl.gz (default: 500)')
    parser.add_argument('--flush-interval-sec', type=float,
                        default=CRAWLER_FLUSH_INTERVAL_SEC,
                        help='Max seconds buffer can stay in memory before flushing (default: 60)')
    args = parser.parse_args()
    if args.review_batches < 1 or args.max_consecutive_errors < 1 or args.review_days < 0:
        parser.error('Review pages and consecutive error limit must be positive; review days >= 0')
    all_apps = load_target_apps(args.targets, args.limit)
    apps = [a for a in all_apps if is_assigned_to_worker(a, args.worker_id, args.workers)]
    assigned_count = len(apps)
    if not 0 <= args.start_index <= assigned_count:
        parser.error('Start index must be within the assigned game list')
    apps = apps[args.start_index:]
    limiter = RateLimiter(args.interval, max_requests=args.max_requests)
    producer = JsonlProducer(
        args.output_dir,
        flush_max_records=args.flush_max_records,
        flush_interval_sec=args.flush_interval_sec,
    )

    def _signal_handler(signum, frame):
        logging.warning("Received signal %s, flushing all buffers...", signum)
        if hasattr(producer, 'flush_all'):
            producer.flush_all()
        raise KeyboardInterrupt(f"Terminated by signal {signum}")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _signal_handler)
        except (ValueError, AttributeError):
            pass

    state = ReviewStateStore(str(Path(args.state_dir) / reviews.REVIEW_STATE_SUBDIR))
    since = reviews.since_from_days(args.review_days)
    started = time.time()
    stats = {'started_at': int(started), 'selected_games': len(all_apps), 'assigned_games': assigned_count,
             'start_index': args.start_index, 'next_game_index': args.start_index, 'sources': args.sources,
             'review_pages': args.review_batches, 'review_since_ts': since,
             'completed_games': 0, 'requests': 0, 'records': 0, 'failures': 0, 'failed_sources': [], 'status': 'running'}
    path = Path(args.output_dir) / f'collection-worker-{args.worker_id}.json'
    functions = {'metadata': metadata.crawl_app_details, 'ccu': ccu.crawl_ccu,
                 'histogram': histogram.crawl_review_histogram}
    write_json(path, stats)
    try:
        if 'store' in args.sources and apps:
            try:
                missing = collect_store(apps, producer, limiter)
            except RuntimeError as exc:
                logging.error('Store snapshot failed: %s', exc)
                missing = list(apps)
            stats['failures'] += len(missing)
            stats['failed_sources'] += [{'appid': a, 'source': 'store'} for a in missing]
            if hasattr(producer, 'flush_all'):
                producer.flush_all()

        for source in [s for s in PER_GAME if s in args.sources]:
            errors = 0
            for i, appid in enumerate(apps):
                if source == 'reviews':
                    ok = reviews.crawl_reviews_for_app(appid, producer, limiter, state,
                                                       max_pages=args.review_batches, since_ts=since)
                else:
                    ok = functions[source](appid, producer, limiter)
                errors = 0 if ok else errors + 1
                stats['failures'] += int(not ok)
                if not ok:
                    stats['failed_sources'].append({'appid': appid, 'source': source})
                if errors >= args.max_consecutive_errors:
                    raise RuntimeError('Consecutive API failures reached the configured limit; check network/API before resuming')
                stats.update(requests=limiter.request_count, records=producer.record_count)
                if (i + 1) % 10 == 0:
                    write_json(path, stats)
                    logging.info('Source %s progress %s/%s games, %s requests, %s records',
                                 source, i + 1, len(apps), limiter.request_count, producer.record_count)
            if hasattr(producer, 'flush_all'):
                producer.flush_all()

        stats['completed_games'] = len(apps)
        stats['next_game_index'] = args.start_index + len(apps)
        stats['status'] = 'finished'
    except RequestBudgetExceeded:
        stats['status'] = 'request_budget_reached'
    except BaseException:
        stats['status'] = 'failed_or_interrupted'
        raise
    finally:
        if hasattr(producer, 'flush_all'):
            producer.flush_all()
        producer.close()
        stats.update(requests=limiter.request_count, records=producer.record_count,
                     ended_at=int(time.time()), elapsed_seconds=round(time.time() - started, 1))
        write_json(path, stats)
        print(stats, flush=True)


if __name__ == '__main__':
    main()
