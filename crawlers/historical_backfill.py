"""Import public 2021 review shards for the selected games, with resumable output."""
import argparse
import hashlib
import json
import logging
import math
import time
from pathlib import Path

import pyarrow.parquet as pq

from crawlers.config import KAFKA_TOPIC_REVIEWS
from crawlers.common.jsonl_producer import JsonlProducer
from crawlers.common.public_datasets import resolve_shards, download_shard, write_json, sha256_file, shard_cache_path
from crawlers.common.targets import load_target_apps

DATASET = 'GianLucaSpagnolo/Steam_Reviews_Dataset_2021'


def pick(row, *names):
    for name in names:
        value = row.get(name)
        if value is not None and value != '' and not (isinstance(value, float) and math.isnan(value)):
            return value
    return None


def as_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).lower()
    if text in ('true', '1'):
        return True
    if text in ('false', '0', '-1'):
        return False
    raise ValueError(f'Invalid boolean {value!r}')


def normalize_review(row):
    appid = pick(row, 'app_id', 'appid')
    review_id = pick(row, 'review_id', 'recommendationid')
    created = pick(row, 'timestamp_created')
    recommended = pick(row, 'recommended', 'voted_up', 'review_score')
    if appid is None or review_id is None or created is None or recommended is None:
        raise ValueError('Historical review is missing appid, review ID, event time or sentiment')
    if isinstance(review_id, float):
        raise ValueError('Review IDs must not be floating-point values')
    if int(appid) <= 0 or int(created) <= 0:
        raise ValueError('Invalid historical appid or timestamp')
    author = pick(row, 'author.steamid', 'author_steamid')
    if isinstance(author, float):
        raise ValueError('SteamIDs must not be floating-point values')
    score = pick(row, 'weighted_vote_score')
    record = {
        'recommendationid': str(review_id), 'appid': int(appid),
        'language': pick(row, 'language'), 'review': pick(row, 'review', 'review_text'),
        'voted_up': as_bool(recommended), 'timestamp_created': int(created),
        'timestamp_updated': int(pick(row, 'timestamp_updated') or created),
        'votes_up': int(pick(row, 'votes_helpful', 'votes_up') or 0),
        'votes_funny': int(pick(row, 'votes_funny') or 0),
        'weighted_vote_score': float(score) if score is not None else None,
        'steam_purchase': as_bool(pick(row, 'steam_purchase')),
        'received_for_free': as_bool(pick(row, 'received_for_free')),
        'written_during_early_access': as_bool(pick(row, 'written_during_early_access')),
        'author_steamid': str(author) if author is not None else None,
        'num_games_owned': pick(row, 'author.num_games_owned', 'author_num_games_owned', 'num_games_owned'),
        'num_reviews': pick(row, 'author.num_reviews', 'author_num_reviews', 'num_reviews'),
        'playtime_forever': pick(row, 'author.playtime_forever', 'author_playtime_forever', 'playtime_forever'),
        'playtime_at_review': pick(row, 'author.playtime_at_review', 'author_playtime_at_review', 'playtime_at_review'),
        'playtime_last_two_weeks': pick(row, 'author.playtime_last_two_weeks', 'author_playtime_last_two_weeks', 'playtime_last_two_weeks'),
        'ingest_timestamp': int(time.time()), 'source': DATASET, 'is_replay': True,
    }
    return record


def import_parquet(file, target_ids, producer, state_path, limit=1000000, source_revision=None, on_progress=None):
    if limit < 1 or not target_ids:
        raise ValueError('Record limit and target set must be nonempty')
    file = Path(file)
    state_path = Path(state_path)
    identity = {'file_sha256': sha256_file(file), 'targets_sha256': hashlib.sha256(json.dumps(sorted(target_ids)).encode()).hexdigest()}
    state = json.loads(state_path.read_text()) if state_path.exists() else {**identity, 'rows_scanned': 0, 'records_imported': 0, 'invalid_rows': 0}
    if any(state.get(k) != value for k, value in identity.items()):
        raise ValueError('Backfill source or target list changed; use a separate state directory')
    skip = state['rows_scanned']
    rows_seen = emitted = invalid = 0

    def commit():
        producer.flush()
        state.update(rows_scanned=rows_seen, records_imported=state['records_imported'] + emitted,
                     invalid_rows=state['invalid_rows'] + invalid, updated_at=int(time.time()))
        write_json(state_path, state)
        if on_progress:
            on_progress(state)

    # Each invocation consumes at most `limit` additional matching reviews.
    initial_imported, initial_invalid = state['records_imported'], state['invalid_rows']
    parquet = pq.ParquetFile(file)
    for batch in parquet.iter_batches(batch_size=2000):
        for row in batch.to_pylist():
            rows_seen += 1
            if rows_seen <= skip:
                continue
            try:
                appid = int(pick(row, 'app_id', 'appid') or 0)
                if appid not in target_ids:
                    continue
                record = normalize_review(row)
            except (ValueError, TypeError, OverflowError):
                invalid += 1
                continue
            record['source_file'] = file.name
            record['source_revision'] = source_revision
            producer.send(KAFKA_TOPIC_REVIEWS, str(appid), record)
            emitted += 1
            if emitted >= limit:
                state['records_imported'], state['invalid_rows'] = initial_imported, initial_invalid
                commit()
                return emitted
        if rows_seen > skip:
            producer.flush()
            state.update(rows_scanned=rows_seen, records_imported=initial_imported + emitted,
                         invalid_rows=initial_invalid + invalid, updated_at=int(time.time()))
            write_json(state_path, state)
            if on_progress:
                on_progress(state)
    state.update(complete=True, rows_scanned=rows_seen, records_imported=initial_imported + emitted,
                 invalid_rows=initial_invalid + invalid, updated_at=int(time.time()))
    producer.flush()
    write_json(state_path, state)
    if on_progress:
        on_progress(state)
    return emitted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets', default='data/target_apps.json')
    parser.add_argument('--target-limit', type=int, default=1000)
    parser.add_argument('--limit', type=int, default=1000000, help='Maximum additional matching reviews per invocation')
    parser.add_argument('--max-download-mb', type=int, default=1024,
                        help='Budget for unfinished shard bytes processed per run (includes cached files); completed shards are skipped')
    parser.add_argument('--cache-dir', default='data/cache/history')
    parser.add_argument('--state-dir', default='data/crawler_state/history')
    parser.add_argument('--output-dir', default='data/raw')
    args = parser.parse_args()
    if args.limit < 1 or args.max_download_mb < 1:
        parser.error('Record and download limits must be positive')
    logging.basicConfig(level=logging.INFO)
    targets = set(load_target_apps(args.targets, args.target_limit))
    manifest = resolve_shards(DATASET)
    producer = JsonlProducer(args.output_dir, batch_size=2000)
    copied = consumed = 0
    artifacts = []
    report_path = Path(args.output_dir) / 'historical-backfill.json'
    status = 'running'
    try:
        for item in manifest['files']:
            if copied >= args.limit:
                break
            cached = shard_cache_path(item, args.cache_dir)
            state_path = Path(args.state_dir) / manifest['revision'] / (cached.stem + '.json')
            if state_path.exists():
                previous = json.loads(state_path.read_text())
                target_hash = hashlib.sha256(json.dumps(sorted(targets)).encode()).hexdigest()
                if previous.get('targets_sha256') != target_hash:
                    raise ValueError('Backfill target list changed; use a separate state directory')
                if previous.get('complete'):
                    if cached.exists() and previous.get('file_sha256') != sha256_file(cached):
                        raise ValueError('Completed backfill source changed; use a separate state directory')
                    logging.info('Skipping completed shard %s', item['filename'])
                    continue
            if consumed + item['size'] > args.max_download_mb * 1024**2:
                status = 'download_budget_reached'
                break
            file = download_shard(item, args.cache_dir, args.max_download_mb * 1024**2 - consumed)
            consumed += item['size']
            prior = json.loads(state_path.read_text())['records_imported'] if state_path.exists() else 0
            before = copied
            def progress(current):
                nonlocal copied
                copied = before + current['records_imported'] - prior
                write_json(report_path, {**manifest, 'records_imported_this_run': copied, 'shard_bytes': consumed,
                                        'artifacts': artifacts, 'active_file': str(file), 'status': 'running'})
                if copied and copied % 100000 == 0:
                    logging.info('Backfill progress: %s reviews committed this run', copied)
            count = import_parquet(file, targets, producer, state_path, args.limit - before, manifest['revision'], progress)
            copied = before + count
            artifacts.append({'file': str(file), 'size': item['size'], 'records': count, 'state': str(state_path)})
            write_json(report_path, {**manifest, 'records_imported_this_run': copied, 'shard_bytes': consumed, 'artifacts': artifacts, 'status': status})
            logging.info('Imported %s/%s reviews', copied, args.limit)
        if status == 'running':
            status = 'record_limit_reached' if copied >= args.limit else 'all_shards_scanned'
    except BaseException:
        status = 'failed_or_interrupted'
        raise
    finally:
        producer.close()
        write_json(report_path, {**manifest, 'records_imported_this_run': copied, 'shard_bytes': consumed,
                                'artifacts': artifacts, 'status': status, 'finished_at': int(time.time())})
        print(f'Backfill: {copied} new reviews, {consumed / 1024**2:.1f} MB of shards, status={status}', flush=True)


if __name__ == '__main__':
    main()
