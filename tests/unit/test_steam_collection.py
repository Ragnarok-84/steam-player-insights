"""Steam-only collection: provenance, pagination, configuration and source isolation."""
import json
import sys

import pytest

from crawlers import build_targets, collect_local, steam_applist_crawler as catalog
from crawlers import steam_app_details_crawler as metadata, steam_ccu_crawler as ccu
from crawlers import steam_reviews_crawler as reviews
from crawlers.common.targets import load_target_apps
from crawlers.common.review_state import ReviewStateStore
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.jsonl_producer import JsonlProducer


class Response:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


def test_catalog_uses_key_and_game_only_pagination(monkeypatch):
    monkeypatch.setattr(catalog, 'STEAM_API_KEY', 'test-secret')
    calls = []
    def get(url, params, timeout):
        calls.append(params)
        if len(calls) == 1:
            return Response({'response': {'apps': [{'appid': 10, 'name': 'A'}],
                                         'have_more_results': True, 'last_appid': 10}})
        return Response({'response': {'apps': [{'appid': 20, 'name': 'B'}]}})
    monkeypatch.setattr(catalog.requests, 'get', get)
    assert [a['appid'] for a in catalog.fetch_all(interval=0)] == [10, 20]
    assert calls[0]['key'] == 'test-secret'
    assert json.loads(calls[1]['input_json'])['last_appid'] == 10
    args = json.loads(calls[0]['input_json'])
    assert args['include_games'] and not any(args[k] for k in (
        'include_dlc', 'include_software', 'include_hardware', 'include_videos'))


def test_catalog_refuses_partial_list(monkeypatch):
    monkeypatch.setattr(catalog, 'STEAM_API_KEY', 'test-secret')
    monkeypatch.setattr(catalog.requests, 'get', lambda *a, **kw: Response({
        'response': {'apps': [{'appid': 10, 'name': 'A'}], 'have_more_results': True, 'last_appid': 10}}))
    with pytest.raises(RuntimeError, match='incomplete'):
        catalog.fetch_all(max_pages=1, interval=0)


def test_catalog_errors_never_expose_key(monkeypatch):
    monkeypatch.setattr(catalog, 'STEAM_API_KEY', 'test-secret')
    def fail(*a, **kw):
        raise catalog.requests.ConnectionError('url?key=test-secret')
    monkeypatch.setattr(catalog.requests, 'get', fail)
    with pytest.raises(RuntimeError) as exc:
        catalog.fetch_all()
    assert 'test-secret' not in str(exc.value)
    assert exc.value.__suppress_context__


NOW = 1_700_000_000


def store_item(appid, reviews, coming_soon=False, released=NOW - 86400):
    release = {'is_coming_soon': True} if coming_soon else {'steam_release_date': released}
    return {'appid': appid, 'name': f'G{appid}', 'success': 1, 'visible': True, 'type': 0, 'release': release,
            'reviews': {'summary_filtered': {'review_count': reviews}}}


def fake_catalog():
    # appid encodes its review count stratum: 1xxx=5, 2xxx=50, 3xxx=500, 4xxx=5000, 5xxx=50000, 6xxx coming soon
    counts = {1: 5, 2: 50, 3: 500, 4: 5000, 5: 50000}
    items = {a: store_item(a, counts[a // 1000]) for k in counts for a in range(k * 1000, k * 1000 + 50)}
    items.update({a: store_item(a, 0, coming_soon=True) for a in range(6000, 6050)})
    return items


def test_stratified_sample_filters_and_fills_equal_quotas():
    items = fake_catalog()
    probe = lambda ids: {a: items[a] for a in ids if a in items}
    picked, report = build_targets.pick_stratified(list(items), probe, 20, 20, 42, 1, NOW, batch_size=37)
    assert len(picked) == 20 and len({e['appid'] for e in picked}) == 20
    assert report['filled'] == {'20-99': 5, '100-999': 5, '1k-9.9k': 5, '10k+': 5}
    assert all(e['review_count'] >= 20 and e['appid'] < 6000 for e in picked)
    again, _ = build_targets.pick_stratified(list(items)[::-1], probe, 20, 20, 42, 1, NOW, batch_size=37)
    assert again == picked  # independent of catalog order
    other, _ = build_targets.pick_stratified(list(items), probe, 20, 20, 43, 1, NOW, batch_size=37)
    assert other != picked


def test_stratified_sample_tops_up_short_strata():
    items = {a: v for a, v in fake_catalog().items() if a // 1000 != 5}  # no 10k+ games at all
    probe = lambda ids: {a: items[a] for a in ids if a in items}
    picked, report = build_targets.pick_stratified(list(items), probe, 20, 20, 42, 1, NOW)
    assert len(picked) == 20 and report['filled']['10k+'] == 0 and report['topped_up'] == 5


def test_top_ccu_skips_known_unreleased_and_low_review_games():
    ranks = [{'rank': i + 1, 'appid': a} for i, a in enumerate([10, 11, 12, 13, 14])]
    items = {10: store_item(10, 9000), 11: store_item(11, 0, coming_soon=True), 12: store_item(12, 3),
             13: store_item(13, 100), 14: store_item(14, 100)}
    picked = build_targets.pick_top_ccu(ranks, items, 2, {10}, 20, 1, NOW)
    assert [e['appid'] for e in picked] == [13, 14] and picked[0]['group'] == 'top_ccu'


def test_build_targets_extends_existing_list_without_duplicates(monkeypatch, tmp_path):
    items = fake_catalog()
    out = tmp_path / 'targets.json'
    monkeypatch.setattr(build_targets, 'fetch_all', lambda: [{'appid': a, 'name': 'x'} for a in items])
    monkeypatch.setattr(build_targets.steam_store, 'fetch_store_items', lambda ids, *a: {a: items[a] for a in ids if a in items})
    monkeypatch.setattr(build_targets.steam_store, 'fetch_most_played',
                        lambda: [{'rank': 1, 'appid': 5000, 'rollup_date': NOW}])
    monkeypatch.setattr(sys, 'argv', ['build_targets', '--output', str(out), '--limit', '9', '--top-ccu', '1'])
    build_targets.main()
    first = json.loads(out.read_text())
    assert len(first) == 9 and first[0]['appid'] == 5000 and first[0]['group'] == 'top_ccu'
    monkeypatch.setattr(sys, 'argv', ['build_targets', '--output', str(out), '--limit', '17', '--top-ccu', '1'])
    build_targets.main()
    second = json.loads(out.read_text())
    assert second[:9] == first  # earlier games keep their place
    assert len({e['appid'] for e in second}) == 17
    manifest = json.loads((tmp_path / 'targets.json.manifest.json').read_text())
    assert manifest['source'] == 'steam_api' and [b['added'] for b in manifest['batches']] == [9, 8]
    assert load_target_apps(out) == [e['appid'] for e in second]


def test_targets_reject_old_dataset_manifest(tmp_path):
    target = tmp_path / 'targets.json'
    target.write_text('[730]')
    (tmp_path / 'targets.json.manifest.json').write_text('{"dataset": "external/catalog"}')
    with pytest.raises(ValueError, match='Steam API'):
        load_target_apps(target)


def test_local_collection_uses_env_defaults_and_only_steam(monkeypatch, tmp_path):
    target = tmp_path / 'targets.json'
    target.write_text('[730, 570]')
    for key, value in {'TARGET_APP_LIST_PATH': str(target), 'OUTPUT_DIR': str(tmp_path / 'raw'),
                       'STATE_DIR': str(tmp_path / 'state'), 'CRAWLER_TARGET_LIMIT': 2,
                       'CRAWLER_TOTAL_WORKERS': 1, 'CRAWLER_WORKER_ID': 0,
                       'CRAWLER_RATE_LIMIT_DELAY': 0, 'CRAWLER_REVIEW_BATCHES': 3, 'CRAWLER_REVIEW_DAYS': 0}.items():
        monkeypatch.setattr(collect_local, key, value)
    batches = []
    monkeypatch.setattr(collect_local.metadata, 'crawl_app_details', lambda *a: True)
    monkeypatch.setattr(collect_local.ccu, 'crawl_ccu', lambda *a: True)
    monkeypatch.setattr(collect_local.histogram, 'crawl_review_histogram', lambda *a: True)
    monkeypatch.setattr(collect_local, 'collect_store', lambda *a: [])
    def review(*a, max_pages, since_ts):
        batches.append(max_pages)
        return True
    monkeypatch.setattr(collect_local.reviews, 'crawl_reviews_for_app', review)
    monkeypatch.setattr(sys, 'argv', ['collect_local'])
    collect_local.main()
    status = json.loads((tmp_path / 'raw/collection-worker-0.json').read_text())
    assert status['completed_games'] == 2 and status['status'] == 'finished'
    assert batches == [3, 3]
    monkeypatch.setattr(sys, 'argv', ['collect_local', '--sources', 'steamspy'])
    with pytest.raises(SystemExit):
        collect_local.main()


def test_steam_response_writes_live_provenance_and_review_cursor(monkeypatch, tmp_path):
    def get(url, params, timeout):
        if '/appdetails' in url:
            return Response({'730': {'success': True, 'data': {'name': 'Game', 'type': 'game'}}})
        if 'GetNumberOfCurrentPlayers' in url:
            return Response({'response': {'result': 1, 'player_count': 123}})
        assert url == reviews.BASE_REVIEW_URL
        assert json.loads(params['input_json'])['cursor'] == '*'
        return Response({'response': {'reviews': [{'recommendationid': '999', 'review': 'good', 'timestamp_created': 5,
                                                   'author': {'steamid': '123'}}], 'cursor': 'next'}})
    monkeypatch.setattr(catalog.requests, 'get', get)
    producer = JsonlProducer(tmp_path / 'raw')
    limiter = RateLimiter(0)
    state = ReviewStateStore(str(tmp_path / 'state'))
    assert metadata.crawl_app_details(730, producer, limiter)
    assert ccu.crawl_ccu(730, producer, limiter)
    assert reviews.crawl_reviews_for_app(730, producer, limiter, state, max_pages=1)
    producer.close()
    import gzip
    records = [json.loads(line) for p in (tmp_path / 'raw').rglob('*.gz')
               for line in gzip.open(p, 'rt')]
    assert len(records) == 3
    assert all(r['source'] == 'steam_live' for r in records)
    assert state.load(730)['backfill']['cursor'] == 'next'
    assert limiter.request_count == 3


def test_b2_keeps_steam_live_metadata(request, monkeypatch, tmp_path):
    monkeypatch.setenv("HIVE_METASTORE_URI", "")
    spark = request.getfixturevalue("spark")
    from processing.batch.b2_clean_raw import clean_games
    producer = JsonlProducer(tmp_path / 'raw')
    producer.send('steam.games.meta', '730', {
        'appid': 730, 'name': 'Steam Game', 'source': 'steam_live',
        'timestamp': 1794412800, 'price': 19.99, 'is_free': False})
    producer.close()
    clean_games(spark, (tmp_path / 'raw').as_uri(), (tmp_path / 'clean').as_uri())
    rows = spark.read.parquet(str(tmp_path / 'clean/games')).collect()
    history = spark.read.parquet(str(tmp_path / 'clean/games_history')).collect()
    assert len(rows) == len(history) == 1
    assert rows[0]['appid'] == 730 and rows[0]['name'] == 'Steam Game'


def test_b2_reads_store_tags_and_daily_review_counts(request, monkeypatch, tmp_path):
    monkeypatch.setenv("HIVE_METASTORE_URI", "")
    spark = request.getfixturevalue("spark")
    from processing.batch.b2_clean_raw import clean_games
    from crawlers.common.steam_store import parse_store_item
    producer = JsonlProducer(tmp_path / 'raw')
    producer.send('steam.games.meta', '730', {'appid': 730, 'name': 'G', 'source': 'steam_live', 'timestamp': 100})
    item = {'appid': 730, 'name': 'G', 'success': 1, 'release': {'steam_release_date': 50},
            'reviews': {'summary_filtered': {'review_count': 42, 'percent_positive': 80}},
            'tags': [{'tagid': 2, 'weight': 5}, {'tagid': 1, 'weight': 9}]}
    producer.send('steam.games.meta', '730', parse_store_item(item, {1: 'Action', 2: 'Indie'}, now=100))
    for ts, up in ((100, 3), (200, 7)):  # two snapshots of the same day: keep the latest
        producer.send('steam.games.meta', '730', {'appid': 730, 'source': 'steam_review_histogram', 'timestamp': ts,
                                                  'daily': [{'date': 86400, 'up': up, 'down': 1}]})
    producer.close()
    clean_games(spark, (tmp_path / 'raw').as_uri(), (tmp_path / 'clean').as_uri())
    game = spark.read.parquet(str(tmp_path / 'clean/games')).collect()[0]
    assert game['tags'] == ['Action', 'Indie'] and game['review_count'] == 42
    daily = spark.read.parquet(str(tmp_path / 'clean/review_daily_counts')).collect()
    assert len(daily) == 1 and daily[0]['reviews_up'] == 7 and daily[0]['reviews_total'] == 8
