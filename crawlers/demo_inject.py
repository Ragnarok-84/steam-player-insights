"""Bơm tình huống bất thường vào Kafka để demo cảnh báo của speed layer (S2, S3).

Review bombing và CCU tăng đột biến hiếm khi xảy ra đúng lúc demo, nên công cụ này tạo sự kiện có gắn nhãn
source=demo_injection cho một game thật trong danh sách theo dõi:

  bombing    N review tiêu cực trong vài phút gần đây       -> S2 cảnh báo REVIEW_BOMBING
  ccu-spike  CCU = factor x baseline cùng khung giờ (B7)     -> S3 cảnh báo CCU_SPIKE (factor < 1: CCU_DROP)

Các sự kiện này chỉ dành cho speed layer: B2 loại mọi bản ghi source=demo_injection, nên không lẫn vào
dữ liệu phân tích dù B1 có chép chúng xuống raw. Mỗi review có recommendationid dạng demo-<uuid>.
"""
import argparse
import random
import time
import uuid
from pathlib import Path

from crawlers.config import KAFKA_TOPIC_REVIEWS, KAFKA_TOPIC_CCU

DEMO_SOURCE = "demo_injection"
NEGATIVE_TEXTS = [
    "Latest update broke the game, constant crashes", "Servers are down again, refund requested",
    "New monetization is predatory, do not buy", "Game is unplayable after the patch",
    "Cheaters everywhere and no response from devs", "Performance tanked, stuttering on every map",
]


def bombing_records(appid, count, minutes, now=None, seed=None):
    rng = random.Random(seed)
    now = int(now or time.time())
    out = []
    for _ in range(count):
        ts = now - rng.randint(0, max(minutes * 60 - 1, 0))
        out.append({
            "recommendationid": f"demo-{uuid.UUID(int=rng.getrandbits(128))}", "appid": int(appid),
            "language": "english", "review": rng.choice(NEGATIVE_TEXTS), "voted_up": False,
            "timestamp_created": ts, "timestamp_updated": ts, "votes_up": 0, "votes_funny": 0,
            "weighted_vote_score": 0.0, "steam_purchase": True, "received_for_free": False,
            "written_during_early_access": False, "author_steamid": None, "num_reviews": 1,
            "playtime_forever": rng.randint(5, 600), "playtime_at_review": rng.randint(5, 600),
            "playtime_last_two_weeks": 0, "ingest_timestamp": now, "source": DEMO_SOURCE,
        })
    return out


def ccu_baseline(lake, appid, hour):
    import pandas as pd
    path = Path(lake) / "curated" / "ccu_baseline"
    if not path.exists():
        raise SystemExit(f"Chưa có {path}; chạy batch (B7) trước hoặc truyền --players")
    b = pd.read_parquet(path)
    b = b[b.appid == int(appid)]
    if b.empty:
        raise SystemExit(f"Game {appid} chưa có baseline CCU; truyền --players")
    same_hour = b[b.hour == hour]
    if same_hour.empty:
        raise SystemExit(f"Game {appid} chưa có baseline cho khung giờ {hour}h UTC (S3 so cùng khung giờ). "
                         f"Các giờ đã có: {sorted(b.hour.unique().tolist())}. Thu CCU thêm hoặc truyền --players")
    return float(same_hour.baseline_ccu.iloc[0])


def ccu_records(appid, players, samples=3, now=None):
    now = int(now or time.time())
    return [{"appid": int(appid), "timestamp": now - i * 60, "player_count": int(players), "source": DEMO_SOURCE}
            for i in range(samples)]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="scenario", required=True)
    b = sub.add_parser("bombing", help="Review bombing cho một game")
    b.add_argument("--appid", type=int, required=True)
    b.add_argument("--count", type=int, default=80)
    b.add_argument("--minutes", type=int, default=20, help="Rải review trong N phút gần nhất")
    c = sub.add_parser("ccu-spike", help="CCU tăng/giảm đột biến cho một game")
    c.add_argument("--appid", type=int, required=True)
    c.add_argument("--factor", type=float, default=3.0, help="Bội số so với baseline cùng khung giờ")
    c.add_argument("--players", type=int, help="Số người chơi cụ thể (bỏ qua baseline)")
    c.add_argument("--lake", default="data/lake")
    args = parser.parse_args()

    from crawlers.common.kafka_producer import SteamKafkaProducer
    producer = SteamKafkaProducer()
    if args.scenario == "bombing":
        records = bombing_records(args.appid, args.count, args.minutes)
        for r in records:
            producer.send(KAFKA_TOPIC_REVIEWS, str(args.appid), r)
        print(f"Sent {len(records)} negative demo reviews for appid {args.appid} to {KAFKA_TOPIC_REVIEWS}")
    else:
        players = args.players
        if players is None:
            base = ccu_baseline(args.lake, args.appid, time.gmtime().tm_hour)
            players = round(base * args.factor)
            print(f"Baseline {base:.0f} at {time.gmtime().tm_hour}h UTC x {args.factor} = {players}")
        records = ccu_records(args.appid, players)
        for r in records:
            producer.send(KAFKA_TOPIC_CCU, str(args.appid), r)
        print(f"Sent {len(records)} demo CCU snapshots ({players} players) for appid {args.appid} to {KAFKA_TOPIC_CCU}")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
