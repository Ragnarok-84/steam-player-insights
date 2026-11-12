"""Vòng phản hồi: thêm game đang lên trên bảng most-played của Steam vào danh sách theo dõi.

Tín hiệu phải đến từ bên ngoài danh sách: B6 chỉ xếp hạng các game đã được crawl, nên không thể tự
phát hiện game mới. Game "đang lên" = mới lọt vào top (chưa có hạng tuần trước) hoặc tăng hạng so
với tuần trước; ưu tiên game mới vào top, rồi game tăng nhiều hạng nhất.

Chỉ thêm, không bao giờ xoá: game cũ giữ nguyên vị trí và dữ liệu đã crawl, game mới có
group=feedback và một mục kind=feedback trong manifest. Chạy hằng ngày sau batch, rồi crawl như thường.
"""
import argparse
import time

from crawlers.build_targets import MAX_TARGETS, load_existing, pick_top_ccu, save_targets
from crawlers.common import steam_store
from crawlers.config import TARGET_APP_LIST_PATH


def rising(ranks):
    """Chart rows that entered the top or climbed since last week, most notable first."""
    def climb(r):
        last = r.get("last_week_rank") or 0
        return float("inf") if last == 0 else last - r["rank"]
    return sorted((r for r in ranks if climb(r) > 0), key=lambda r: (-climb(r), r["rank"]))


def update(targets_path, top, min_reviews, ranks=None, items=None, now=None, max_targets=MAX_TARGETS):
    """Append up to `top` rising games. Returns the added entries."""
    existing, manifest = load_existing(targets_path)
    if not existing or manifest.get("source") != "steam_api":
        raise SystemExit(f"{targets_path} must be a list built by crawlers.build_targets")
    room = min(top, max_targets - len(existing))
    if room <= 0:
        print(f"Target list is full ({len(existing)}/{max_targets}); nothing added")
        return []
    now = now or time.time()
    ranks = steam_store.fetch_most_played() if ranks is None else ranks
    candidates = rising(ranks)
    known = {int(e["appid"]) for e in existing}
    if items is None:
        items = steam_store.fetch_store_items([r["appid"] for r in candidates if int(r["appid"]) not in known])
    batch = len(manifest.get("batches", [])) + 1
    added = pick_top_ccu(candidates, items, room, known, min_reviews, batch, now, group="feedback")
    if added:
        save_targets(targets_path, existing, added, manifest, {
            "batch": batch, "kind": "feedback", "retrieved_at": int(now), "min_reviews": min_reviews,
            "top_ccu_rollup_date": ranks[0].get("rollup_date") if ranks else None,
            "rising_candidates": len(candidates), "added": len(added)})
    return added


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--targets", default=TARGET_APP_LIST_PATH)
    parser.add_argument("--top", type=int, default=5, help="Số game mới tối đa mỗi lần chạy")
    parser.add_argument("--min-reviews", type=int, default=20)
    args = parser.parse_args()
    if args.top < 1 or args.min_reviews < 1:
        parser.error("--top and --min-reviews must be positive")
    added = update(args.targets, args.top, args.min_reviews)
    print(f"Added {len(added)} rising games: "
          + ", ".join(f"{e['appid']} {e['name']} (#{e['ccu_rank']}, last week #{e['last_week_rank'] or '-'})"
                      for e in added))


if __name__ == "__main__":
    main()
