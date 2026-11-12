"""Feedback loop: rising games from the Steam chart are appended; nothing is ever removed."""
import json

from crawlers.build_targets import save_targets
from crawlers.update_targets_from_trending import rising, update
from crawlers.common.targets import load_target_apps

NOW = 1_700_000_000


def item(appid, reviews=500, coming_soon=False):
    release = {"is_coming_soon": True} if coming_soon else {"steam_release_date": NOW - 86400}
    return {"appid": appid, "name": f"G{appid}", "success": 1, "visible": True, "type": 0, "release": release,
            "reviews": {"summary_filtered": {"review_count": reviews}}}


def test_rising_prefers_new_entries_then_biggest_climb():
    ranks = [{"rank": 1, "appid": 1, "last_week_rank": 1},   # flat
             {"rank": 2, "appid": 2, "last_week_rank": 9},   # +7
             {"rank": 3, "appid": 3, "last_week_rank": 0},   # new
             {"rank": 4, "appid": 4, "last_week_rank": 2},   # falling
             {"rank": 5, "appid": 5, "last_week_rank": 6}]   # +1
    assert [r["appid"] for r in rising(ranks)] == [3, 2, 5]


def test_feedback_appends_only_and_records_batch(tmp_path):
    path = tmp_path / "targets.json"
    old = [{"appid": a, "group": "stratified"} for a in (10, 20, 30)]
    save_targets(path, [], old, {}, {"batch": 1, "kind": "build", "added": 3})
    ranks = [{"rank": 1, "appid": 20, "last_week_rank": 0},              # already tracked
             {"rank": 2, "appid": 40, "last_week_rank": 50},             # rising
             {"rank": 3, "appid": 50, "last_week_rank": 0},              # new entry, unreleased
             {"rank": 4, "appid": 60, "last_week_rank": 9},              # rising, too few reviews
             {"rank": 5, "appid": 70, "last_week_rank": 7},              # rising
             {"rank": 6, "appid": 80, "last_week_rank": 1}]              # falling
    items = {40: item(40), 50: item(50, coming_soon=True), 60: item(60, reviews=3), 70: item(70), 80: item(80)}
    added = update(path, top=5, min_reviews=20, ranks=ranks, items=items, now=NOW)
    assert [e["appid"] for e in added] == [40, 70] and all(e["group"] == "feedback" for e in added)
    assert load_target_apps(path) == [10, 20, 30, 40, 70]
    manifest = json.loads((tmp_path / "targets.json.manifest.json").read_text())
    assert [b["kind"] for b in manifest["batches"]] == ["build", "feedback"]
    assert manifest["selected_games"] == 5
    assert update(path, top=5, min_reviews=20, ranks=ranks, items=items, now=NOW) == []  # idempotent


def test_feedback_respects_cap_without_removing(tmp_path):
    path = tmp_path / "targets.json"
    save_targets(path, [], [{"appid": a} for a in range(1, 5)], {}, {"batch": 1, "kind": "build"})
    ranks = [{"rank": i, "appid": 100 + i, "last_week_rank": 0} for i in range(1, 4)]
    items = {100 + i: item(100 + i) for i in range(1, 4)}
    added = update(path, top=3, min_reviews=20, ranks=ranks, items=items, now=NOW, max_targets=5)
    assert len(added) == 1 and load_target_apps(path) == [1, 2, 3, 4, 101]
    assert update(path, top=3, min_reviews=20, ranks=ranks, items=items, now=NOW, max_targets=5) == []
