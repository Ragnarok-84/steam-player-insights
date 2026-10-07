"""A bounded local collection pass over the selected games; no Kafka required."""
import argparse
import logging
import time
from pathlib import Path

from crawlers import steam_app_details_crawler as metadata
from crawlers import steam_ccu_crawler as ccu
from crawlers import steam_reviews_crawler as reviews
from crawlers import steamspy_crawler as steamspy
from crawlers.common.cursor_manager import CursorManager
from crawlers.common.jsonl_producer import JsonlProducer
from crawlers.common.public_datasets import write_json
from crawlers.common.rate_limiter import RateLimiter, RequestBudgetExceeded
from crawlers.common.targets import load_target_apps, is_assigned_to_worker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets', default='data/target_apps.json')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--sources', nargs='+', choices=['metadata', 'ccu', 'reviews', 'steamspy'], default=['metadata', 'ccu', 'reviews'])
    parser.add_argument('--output-dir', default='data/raw')
    parser.add_argument('--state-dir', default='data/crawler_state')
    parser.add_argument('--interval', type=float, default=1.5)
    parser.add_argument('--max-requests', type=int, default=3000)
    parser.add_argument('--review-batches', type=int, default=1)
    parser.add_argument('--max-consecutive-errors', type=int, default=5)
    parser.add_argument('--worker-id', type=int, default=0)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--start-index', type=int, default=0,
                        help='Skip this many games assigned to the worker; use next_game_index from an interrupted pass')
    args = parser.parse_args()
    if args.review_batches < 1 or args.max_consecutive_errors < 1:
        parser.error('Review batches and consecutive error limit must be positive')
    all_apps = load_target_apps(args.targets, args.limit)
    apps = [a for a in all_apps if is_assigned_to_worker(a, args.worker_id, args.workers)]
    assigned_count = len(apps)
    if not 0 <= args.start_index <= assigned_count:
        parser.error('Start index must be within the assigned game list')
    apps = apps[args.start_index:]
    limiter = RateLimiter(args.interval, max_requests=args.max_requests)
    producer = JsonlProducer(args.output_dir)
    state = CursorManager(str(Path(args.state_dir) / 'reviews_v1'))
    started = time.time()
    stats = {'started_at': int(started), 'selected_games': len(all_apps), 'assigned_games': assigned_count,
             'start_index': args.start_index, 'next_game_index': args.start_index, 'sources': args.sources,
             'completed_games': 0, 'requests': 0, 'records': 0, 'failures': 0, 'failed_sources': [], 'status': 'running'}
    path = Path(args.output_dir) / f'collection-worker-{args.worker_id}.json'
    errors = 0
    functions = {'metadata': metadata.crawl_app_details, 'ccu': ccu.crawl_ccu, 'steamspy': steamspy.crawl_steamspy}
    write_json(path, stats)
    try:
        for appid in apps:
            for source in args.sources:
                if source == 'reviews':
                    ok = reviews.crawl_reviews_for_app(appid, producer, limiter, state, max_batches=args.review_batches)
                else:
                    ok = functions[source](appid, producer, limiter)
                producer.flush()
                errors = 0 if ok else errors + 1
                stats['failures'] += int(not ok)
                if not ok:
                    stats['failed_sources'].append({'appid': appid, 'source': source})
                if errors >= args.max_consecutive_errors:
                    raise RuntimeError('Consecutive API failures reached the configured limit; check network/API before resuming')
            stats['completed_games'] += 1
            stats.update(requests=limiter.request_count, records=producer.record_count,
                         next_game_index=args.start_index + stats['completed_games'])
            write_json(path, stats)
            if stats['completed_games'] % 10 == 0:
                logging.info('Progress %s/%s games, %s requests, %s records', stats['completed_games'], len(apps), limiter.request_count, producer.record_count)
        stats['status'] = 'finished'
    except RequestBudgetExceeded:
        stats['status'] = 'request_budget_reached'
    except BaseException:
        stats['status'] = 'failed_or_interrupted'
        raise
    finally:
        producer.close()
        stats.update(requests=limiter.request_count, records=producer.record_count,
                     ended_at=int(time.time()), elapsed_seconds=round(time.time() - started, 1))
        write_json(path, stats)
        print(stats, flush=True)


if __name__ == '__main__':
    main()
