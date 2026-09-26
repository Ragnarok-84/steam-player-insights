import os
from dotenv import load_dotenv

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TOPIC_REVIEWS = os.getenv("KAFKA_TOPIC_REVIEWS", "steam.reviews.raw")
KAFKA_TOPIC_CCU = os.getenv("KAFKA_TOPIC_CCU", "steam.players.ccu")
KAFKA_TOPIC_META = os.getenv("KAFKA_TOPIC_META", "steam.games.meta")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "steam.alerts")

CRAWLER_WORKER_ID = int(os.getenv("CRAWLER_WORKER_ID", "0"))
CRAWLER_TOTAL_WORKERS = int(os.getenv("CRAWLER_TOTAL_WORKERS", "1"))
CRAWLER_RATE_LIMIT_DELAY = float(os.getenv("CRAWLER_RATE_LIMIT_DELAY", "1.5"))

STATE_DIR = os.getenv("CURSOR_STATE_DIR", "./data/crawler_state")
TARGET_APP_LIST_PATH = os.getenv("TARGET_APP_LIST_PATH", "./data/target_apps.json")
