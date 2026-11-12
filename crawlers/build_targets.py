"""Build or extend the target game list from Steam: top-CCU games plus a stratified sample by review count.

Pipeline: Steam catalog (GetAppList) -> seeded shuffle -> probe via GetItems in batches of 200 ->
keep released games with >= --min-reviews -> fill equal quotas per review-count stratum.
An existing list is extended, never reshuffled: its games stay first and are excluded from new picks,
so a later, larger crawl reuses all data already collected.
"""
import argparse
import hashlib
import json
import random
import time
from pathlib import Path

from crawlers.config import TARGET_APP_LIST_PATH, CRAWLER_TARGET_LIMIT
from crawlers.common.files import write_json
from crawlers.common import steam_store
from crawlers.steam_applist_crawler import fetch_all, URL as APPLIST_URL

MAX_TARGETS = 2000
# (label, min reviews inclusive, max reviews exclusive)
STRATA = [("20-99", 20, 100), ("100-999", 100, 1000), ("1k-9.9k", 1000, 10000), ("10k+", 10000, None)]


def stratum_of(count, strata=STRATA):
    for label, lo, hi in strata:
        if count >= lo and (hi is None or count < hi):
            return label
    return None


def strata_for(min_reviews):
    """Strata clipped to the minimum review threshold; strata entirely below it are dropped."""
    out = []
    for label, lo, hi in STRATA:
        if hi is not None and hi <= min_reviews:
            continue
        if lo < min_reviews:
            lo, label = min_reviews, (f"{min_reviews}-{hi - 1}" if hi else f"{min_reviews}+")
        out.append((label, lo, hi))
    return out


def quotas(total, labels):
    base, extra = divmod(total, len(labels))
    # Remainder goes to the larger strata, which carry most of the activity.
    return {label: base + (1 if i >= len(labels) - extra else 0) for i, label in enumerate(labels)}


def entry(item, group, stratum, batch, now, **extra):
    return {"appid": int(item["appid"]), "name": item.get("name", ""), "group": group, "stratum": stratum,
            "review_count": steam_store.review_count(item), "batch": batch, "added_at": int(now), **extra}


def pick_top_ccu(ranks, items, n, exclude, min_reviews, batch, now, group="top_ccu"):
    """Take up to n eligible games from chart rows, in the order given."""
    picked = []
    for r in ranks:
        if len(picked) >= n:
            break
        item = items.get(int(r["appid"]))
        if int(r["appid"]) in exclude or item is None or not steam_store.is_released_game(item, now):
            continue
        if steam_store.review_count(item) < min_reviews:
            continue
        picked.append(entry(item, group, stratum_of(steam_store.review_count(item)), batch, now,
                            ccu_rank=r["rank"], last_week_rank=r.get("last_week_rank"), peak_ccu=r.get("peak_in_game")))
    return picked


def pick_stratified(candidates, probe, n, min_reviews, seed, batch, now, max_probe=30000, batch_size=200):
    """Probe candidates in seeded random order; the first eligible games per stratum form a random sample of it.

    probe(appids) -> {appid: store item}. Short strata are topped up from the others so the total stays n.
    """
    strata = strata_for(min_reviews)
    want = quotas(n, [s[0] for s in strata])
    order = sorted(set(int(a) for a in candidates))
    random.Random(seed).shuffle(order)
    chosen = {label: [] for label in want}
    spare = []
    probed = 0
    for i in range(0, min(len(order), max_probe), batch_size):
        if all(len(chosen[l]) >= want[l] for l in want):
            break
        chunk = order[i:i + batch_size]
        items = probe(chunk)
        probed += len(chunk)
        for appid in chunk:  # keep the seeded order, not the API's order
            item = items.get(appid)
            if item is None or not steam_store.is_released_game(item, now):
                continue
            count = steam_store.review_count(item)
            label = stratum_of(count, strata)
            if label is None:
                continue
            e = entry(item, "stratified", label, batch, now)
            (chosen[label] if len(chosen[label]) < want[label] else spare).append(e)
    picked = [e for label in want for e in chosen[label]]
    shortfall = n - len(picked)
    picked += spare[:shortfall]
    counts = {label: len(chosen[label]) for label in want}
    return picked, {"quota": want, "filled": counts, "topped_up": min(shortfall, len(spare)), "probed": probed}


def load_existing(path):
    path = Path(path)
    if not path.exists():
        return [], {}
    entries = json.loads(path.read_text(encoding="utf-8-sig"))
    entries = [e if isinstance(e, dict) else {"appid": int(e)} for e in entries]
    manifest_path = Path(str(path) + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    return entries, manifest


SELECTION = ("group=top_ccu: highest-ranked released games on the Steam most-played chart; "
             "group=stratified: seeded random sample of released catalog games with >= min_reviews, "
             "equal quota per review-count stratum; "
             "group=feedback: games rising on the most-played chart, added by crawlers.update_targets_from_trending. "
             "Earlier batches are kept unchanged and first; games are never removed.")


def save_targets(output, existing, added, manifest, batch_info):
    """Append-only write of the list plus a manifest batch entry."""
    output = Path(output)
    write_json(output, existing + added)
    batches = manifest.get("batches", []) + [batch_info]
    write_json(str(output) + ".manifest.json", {
        "source": "steam_api",
        "endpoints": [APPLIST_URL, steam_store.ITEMS_URL, steam_store.MOST_PLAYED_URL],
        "selected_games": len(existing) + len(added),
        "selection": SELECTION,
        "batches": batches,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "note": "Not representative of the whole catalog: games under min_reviews and unreleased games are excluded."})


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=CRAWLER_TARGET_LIMIT,
                        help="Total games in the list after this run (existing games included)")
    parser.add_argument("--top-ccu", type=int, default=20, help="New games taken from the Steam most-played chart")
    parser.add_argument("--min-reviews", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-probe", type=int, default=30000, help="Catalog games probed at most for the sample")
    parser.add_argument("--output", default=TARGET_APP_LIST_PATH)
    parser.add_argument("--fresh", action="store_true", help="Ignore an existing list instead of extending it")
    args = parser.parse_args()
    if not 1 <= args.limit <= MAX_TARGETS or args.top_ccu < 0 or args.min_reviews < 1:
        parser.error(f"Need 1 <= --limit <= {MAX_TARGETS}, --top-ccu >= 0 and --min-reviews >= 1")

    existing, manifest = ([], {}) if args.fresh else load_existing(args.output)
    if manifest and manifest.get("source") != "steam_api":
        parser.error("Existing list was not built from the Steam API; use --fresh or another --output")
    known = {int(e["appid"]) for e in existing}
    need = args.limit - len(existing)
    if need <= 0:
        print(f"{args.output} already has {len(existing)} games (>= --limit {args.limit}); nothing to add")
        return
    now = time.time()
    batch = len(manifest.get("batches", [])) + 1

    # Fetch everything before writing; API errors must not publish a partial list.
    ranks = steam_store.fetch_most_played() if args.top_ccu else []
    rank_items = steam_store.fetch_store_items([r["appid"] for r in ranks]) if ranks else {}
    top = pick_top_ccu(sorted(ranks, key=lambda r: r["rank"]), rank_items, min(args.top_ccu, need), known,
                       args.min_reviews, batch, now)
    known |= {e["appid"] for e in top}

    catalog = fetch_all()
    candidates = [int(a["appid"]) for a in catalog if a.get("name", "").strip() and int(a["appid"]) not in known]
    sample, report = pick_stratified(candidates, steam_store.fetch_store_items, need - len(top),
                                     args.min_reviews, args.seed, batch, now, args.max_probe)
    added = top + sample
    if not added:
        raise SystemExit("No eligible games found; nothing written. Raise --max-probe or lower --min-reviews")
    if len(added) < need:
        print(f"Warning: only {len(added)}/{need} eligible games found; raise --max-probe or lower --min-reviews")

    save_targets(args.output, existing, added, manifest, {
        "batch": batch, "kind": "build", "retrieved_at": int(now), "seed": args.seed, "min_reviews": args.min_reviews,
        "catalog_games": len(catalog), "added": len(added), "top_ccu": len(top),
        "top_ccu_rollup_date": ranks[0]["rollup_date"] if ranks else None, "stratified": report})
    print(f"Added {len(added)} games (top_ccu={len(top)}, stratified={len(sample)} {report['filled']}); "
          f"total {len(existing) + len(added)} in {args.output}")


if __name__ == "__main__":
    main()
