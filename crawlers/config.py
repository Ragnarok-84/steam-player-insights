import os
from dotenv import load_dotenv

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
if STEAM_API_KEY == "your_steam_api_key_here":
    STEAM_API_KEY = ""
# Ghi đè để trỏ tới mock server khi test FT6 (lỗi 429)
STEAM_WEB_API_BASE = os.getenv("STEAM_WEB_API_BASE", "https://api.steampowered.com").rstrip("/")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TOPIC_REVIEWS = os.getenv("KAFKA_TOPIC_REVIEWS", "steam.reviews.raw")
KAFKA_TOPIC_CCU = os.getenv("KAFKA_TOPIC_CCU", "steam.players.ccu")
KAFKA_TOPIC_META = os.getenv("KAFKA_TOPIC_META", "steam.games.meta")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "steam.alerts")

CRAWLER_WORKER_ID = int(os.getenv("CRAWLER_WORKER_ID", "0"))
CRAWLER_TOTAL_WORKERS = int(os.getenv("CRAWLER_TOTAL_WORKERS", "1"))
CRAWLER_RATE_LIMIT_DELAY = float(os.getenv("CRAWLER_RATE_LIMIT_DELAY", "1.5"))
# Thời gian nghỉ (giây) khi API trả 429 trước khi gọi tiếp (FT6 đặt nhỏ để test nhanh)
CRAWLER_RATE_LIMIT_COOLDOWN = float(os.getenv("CRAWLER_RATE_LIMIT_COOLDOWN", "30"))

STATE_DIR = os.getenv("CURSOR_STATE_DIR", "./data/crawler_state")
TARGET_APP_LIST_PATH = os.getenv("TARGET_APP_LIST_PATH", "./data/target_apps.json")
CRAWLER_OUTPUT = os.getenv("CRAWLER_OUTPUT", "kafka")
OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "./data/raw")
CRAWLER_TARGET_LIMIT = int(os.getenv("CRAWLER_TARGET_LIMIT", "1000"))
CRAWLER_REVIEW_BATCHES = int(os.getenv("CRAWLER_REVIEW_BATCHES", "1"))
