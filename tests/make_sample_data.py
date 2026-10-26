"""Sinh dữ liệu mẫu cùng format crawlers.collect_local (data/raw/steam_*/dt=*/part-*.jsonl.gz).

Dùng để chạy thử pipeline khi chưa có dữ liệu thật, và để kiểm tra luật cảnh báo:
- game BOMB_APPID có đợt review tiêu cực dồn dập trong giờ cuối
- game SPIKE_APPID có CCU tăng gấp 3 ở giờ cuối
- mọi game có một đợt giảm giá bắt đầu giữa kỳ (B6 event_impact)
"""
import argparse
import gzip
import json
import random
from pathlib import Path

BOMB_APPID = 1001
SPIKE_APPID = 1002
WORDS_POS = "great fun gameplay story beautiful graphics worth price combat amazing".split()
WORDS_NEG = "lag crash bug server disconnect overpriced cheater boring broken refund".split()


def write(root: Path, folder: str, dt: str, records):
    d = root / folder / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    with gzip.open(d / f"part-{len(list(d.iterdir())):05d}.jsonl.gz", "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def review(rid, appid, ts, up, rng, lang="english"):
    words = rng.sample(WORDS_POS if up else WORDS_NEG, 5)
    return {
        "recommendationid": str(rid), "appid": appid, "language": lang, "review": " ".join(words) + " game",
        "voted_up": up, "timestamp_created": ts, "timestamp_updated": ts, "votes_up": rng.randint(0, 20),
        "votes_funny": 0, "weighted_vote_score": 0.5, "steam_purchase": True, "received_for_free": False,
        "written_during_early_access": False, "author_steamid": str(7656119 * 10 ** 10 + rid),
        "num_games_owned": None, "num_reviews": rng.randint(1, 50), "playtime_forever": rng.randint(10, 30000),
        "playtime_at_review": rng.choice([5, 60, 300, 1500, 6000, 15000]), "playtime_last_two_weeks": 0,
        "ingest_timestamp": ts + 60, "source": "sample",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="data/sample_raw")
    p.add_argument("--games", type=int, default=30)
    p.add_argument("--days", type=int, default=21)
    p.add_argument("--reviews-per-hour", type=float, default=2.0)
    p.add_argument("--end-ts", type=int, default=1_760_000_400, help="epoch giây của giờ cuối")
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()
    rng = random.Random(a.seed)
    root = Path(a.out)
    end = a.end_ts - a.end_ts % 3600
    start = end - a.days * 86400
    apps = [1000 + i for i in range(a.games)]
    rid = 1
    for day in range(a.days):
        day_start = start + day * 86400
        dt = __import__("datetime").datetime.utcfromtimestamp(day_start).strftime("%Y-%m-%d")
        reviews, ccu, meta = [], [], []
        for appid in apps:
            base_pos = 0.55 + (appid % 7) * 0.05
            growth = 1 + (day / a.days) * (2 if appid % 5 == 0 else 0)
            for h in range(24):
                ts0 = day_start + h * 3600
                n = rng.randint(0, int(a.reviews_per_hour * 2 * growth))
                for _ in range(n):
                    reviews.append(review(rid, appid, ts0 + rng.randint(0, 3599), rng.random() < base_pos, rng,
                                          lang=rng.choice(["english"] * 4 + ["schinese", "russian"])))
                    rid += 1
                players = int((5000 + appid * 3) * (0.6 + 0.4 * abs(12 - h) / 12) * growth)
                if appid == SPIKE_APPID and ts0 == end - 3600:
                    players *= 3
                for m in (0, 30):
                    ccu.append({"appid": appid, "timestamp": ts0 + m * 60, "player_count": players + rng.randint(-50, 50)})
                if appid == BOMB_APPID and ts0 == end - 3600:
                    for _ in range(80):
                        reviews.append(review(rid, appid, ts0 + rng.randint(0, 3599), False, rng))
                        rid += 1
            discount = 50 if day >= a.days // 2 else 0
            meta.append({"appid": appid, "name": f"Sample Game {appid}", "type": "game", "is_free": False,
                         "developer": [f"Studio {appid % 4}"], "publisher": ["Pub"],
                         "genres": [["Action", "RPG", "Indie", "Strategy"][appid % 4]], "categories": [],
                         "price": round(19.99 * (1 - discount / 100), 2), "initial_price": 19.99,
                         "discount_percent": discount, "currency": "USD", "release_date": "Jan 1, 2020",
                         "platforms": {"windows": True}, "metacritic_score": None, "required_age": 0,
                         "timestamp": day_start + 3600})
            meta.append({"appid": appid, "name": f"Sample Game {appid}", "developer": f"Studio {appid % 4}",
                         "owners_range": "100,000 .. 200,000", "average_forever": 600, "median_forever": 300,
                         "tags": {"Action": 100, "Multiplayer": 50}, "source": "steamspy", "timestamp": day_start + 7200})
        write(root, "steam_reviews_raw", dt, reviews)
        write(root, "steam_players_ccu", dt, ccu)
        write(root, "steam_games_meta", dt, meta)
    print(f"Wrote {rid - 1} reviews for {a.games} games x {a.days} days to {root}")


if __name__ == "__main__":
    main()
