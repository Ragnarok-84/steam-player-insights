"""Incremental review crawling: every review is emitted exactly once across runs, budgets and failures."""
import random
from collections import Counter

from crawlers import steam_reviews_crawler as reviews
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.review_state import ReviewStateStore

PAGE = 3


class FakeSteam:
    """Newest-first pages with position cursors, like GetAppReviews(filter=recent)."""

    def __init__(self):
        self.items = []  # (ts, id)
        self.next_id = 1
        self.fail_rate = 0.0
        self.rng = random.Random(0)

    def add(self, ts):
        self.items.append((ts, self.next_id))
        self.next_id += 1

    def fetch(self, appid, cursor, limiter, retries):
        if self.rng.random() < self.fail_rate:
            return None
        ordered = sorted(self.items, key=lambda x: (-x[0], -x[1]))
        if cursor != "*":
            ts, rid = map(int, cursor.split(":"))
            ordered = [x for x in ordered if (x[0], x[1]) < (ts, rid)]
        page = ordered[:PAGE]
        nxt = f"{page[-1][0]}:{page[-1][1]}" if page else cursor
        return [{"recommendationid": str(i), "timestamp_created": t} for t, i in page], nxt


class Sink:
    def __init__(self):
        self.ids = []

    def send(self, topic, key, value):
        self.ids.append(int(value["recommendationid"]))

    def flush(self):
        pass


def run(steam, store, sink, pages, since=None):
    return reviews.crawl_reviews_for_app(1, sink, RateLimiter(0), store, max_pages=pages, since_ts=since)


def setup(monkeypatch, tmp_path):
    steam = FakeSteam()
    monkeypatch.setattr(reviews, "fetch_page", steam.fetch)
    return steam, ReviewStateStore(str(tmp_path)), Sink()


def test_second_run_fetches_only_new_reviews(monkeypatch, tmp_path):
    steam, store, sink = setup(monkeypatch, tmp_path)
    for ts in range(91, 101):
        steam.add(ts)
    run(steam, store, sink, pages=2)
    assert len(sink.ids) == 6
    for ts in range(101, 105):
        steam.add(ts)
    run(steam, store, sink, pages=2)  # head only: the 4 new ones
    assert sorted(sink.ids) == list(range(5, 15))
    run(steam, store, sink, pages=10)  # head finds nothing new, backfill finishes the history
    assert sorted(sink.ids) == list(range(1, 15))
    assert store.load(1)["history_exhausted"]
    calls_before = len(sink.ids)
    run(steam, store, sink, pages=10)
    assert len(sink.ids) == calls_before


def test_unfinished_head_becomes_gap_without_holes(monkeypatch, tmp_path):
    steam, store, sink = setup(monkeypatch, tmp_path)
    for ts in range(1, 5):
        steam.add(ts)
    run(steam, store, sink, pages=10)
    for ts in range(10, 17):
        steam.add(ts)
    run(steam, store, sink, pages=1)  # budget ends inside the new reviews
    assert len(store.load(1)["gaps"]) == 1
    for ts in range(20, 22):
        steam.add(ts)
    run(steam, store, sink, pages=10)
    assert sorted(sink.ids) == list(range(1, steam.next_id))
    assert store.load(1)["gaps"] == []


def test_backfill_stops_at_window_and_extends_later(monkeypatch, tmp_path):
    steam, store, sink = setup(monkeypatch, tmp_path)
    for ts in range(1, 21):
        steam.add(ts)
    run(steam, store, sink, pages=50, since=15)
    assert sorted(int(i) for i in sink.ids) == list(range(15, 21))  # ts 15..20 have ids 15..20
    assert store.load(1)["backfill_reached_ts"] == 15
    run(steam, store, sink, pages=50, since=16)  # narrower window: nothing to do, no request
    assert len(sink.ids) == 6
    run(steam, store, sink, pages=50, since=None)  # full history now
    assert sorted(sink.ids) == list(range(1, 21))


def test_random_runs_emit_each_review_exactly_once(monkeypatch, tmp_path):
    rng = random.Random(7)
    for trial in range(30):
        steam, store, sink = setup(monkeypatch, tmp_path / str(trial))
        steam.rng = random.Random(trial)
        ts = 1000
        for _ in range(rng.randint(0, 40)):
            ts += rng.choice([0, 1, 1, 2])
            steam.add(ts)
        steam.fail_rate = 0.2
        for _ in range(rng.randint(1, 12)):
            for _ in range(rng.randint(0, 8)):
                ts += rng.choice([0, 1, 3])  # 0 = same second as the current newest
                steam.add(ts)
            run(steam, store, sink, pages=rng.randint(1, 3))
        steam.fail_rate = 0.0
        run(steam, store, sink, pages=10_000)
        counts = Counter(sink.ids)
        assert all(c == 1 for c in counts.values()), f"duplicates in trial {trial}"
        assert set(counts) == {i for _, i in steam.items}, f"missing reviews in trial {trial}"


def test_game_without_reviews_is_polled_again(monkeypatch, tmp_path):
    steam, store, sink = setup(monkeypatch, tmp_path)
    run(steam, store, sink, pages=5)
    assert not store.load(1)["history_exhausted"]
    steam.add(10)
    run(steam, store, sink, pages=5)
    assert sink.ids == [1]
