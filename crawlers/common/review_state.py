"""Per-game review crawl state that makes repeated runs incremental and duplicate-free.

GetAppReviews(filter=recent) pages from newest to oldest. Each run works through segments in order:
  head     cursor '*' down to the newest review seen last run   -> only reviews written since then
  gaps     unfinished heads of earlier runs (page budget ran out) -> no hole between runs
  backfill continues towards older reviews until --review-days   -> history, resumable, extendable
Every segment carries an exclusive upper bound (upper_ts/upper_ids) and a lower stop (stop_ts/stop_ids),
so a page fetched twice never emits the same review twice. Two segments can only overlap inside one second
that is a segment boundary (same-second reviews spread over several pages), so boundary_ids keeps the ids
already emitted at those seconds.
"""
import json
from pathlib import Path

from crawlers.common.files import write_json

VERSION = 2


def new_state(appid):
    return {"appid": int(appid), "version": VERSION, "newest_ts": None, "newest_ids": [], "gaps": [],
            "backfill": None, "backfill_reached_ts": None, "history_exhausted": False, "boundary_ids": {},
            "oldest_ts": None, "reviews_emitted": 0, "updated_at": None}


class ReviewStateStore:
    def __init__(self, state_dir):
        self.root = Path(state_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, appid):
        return self.root / f"reviews_{int(appid)}.json"

    def load(self, appid):
        path = self._path(appid)
        if not path.exists():
            return new_state(appid)
        state = json.loads(path.read_text(encoding="utf-8"))
        if state.get("version") != VERSION:
            raise ValueError(f"Unsupported review state version in {path}; use a new --state-dir")
        return {**new_state(appid), **state}

    def save(self, appid, state):
        write_json(self._path(appid), state)


def segment(cursor="*", upper_ts=None, upper_ids=(), stop_ts=None, stop_ids=()):
    return {"cursor": cursor, "upper_ts": upper_ts, "upper_ids": list(upper_ids),
            "stop_ts": stop_ts, "stop_ids": list(stop_ids)}


def filter_page(seg, reviews):
    """Return (reviews to emit, reached_stop) for one page inside a segment."""
    emit, reached_stop = [], False
    upper_ids, stop_ids = set(seg["upper_ids"]), set(seg["stop_ids"])
    for r in reviews:
        ts, rid = int(r["timestamp_created"]), str(r["recommendationid"])
        if seg["upper_ts"] is not None and (ts > seg["upper_ts"] or (ts == seg["upper_ts"] and rid in upper_ids)):
            continue  # already emitted by this segment earlier
        if seg["stop_ts"] is not None and ts < seg["stop_ts"]:
            reached_stop = True
            continue
        if seg["stop_ts"] is not None and ts == seg["stop_ts"] and rid in stop_ids:
            continue
        emit.append(r)
    return emit, reached_stop


def lower_bound(seg, emitted):
    """Move the segment's exclusive upper bound down to the oldest emitted review."""
    for r in emitted:
        ts, rid = int(r["timestamp_created"]), str(r["recommendationid"])
        if seg["upper_ts"] is None or ts < seg["upper_ts"]:
            seg["upper_ts"], seg["upper_ids"] = ts, [rid]
        elif ts == seg["upper_ts"] and rid not in seg["upper_ids"]:
            seg["upper_ids"].append(rid)
