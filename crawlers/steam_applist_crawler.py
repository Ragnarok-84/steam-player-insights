"""Tải danh sách toàn bộ app Steam (hằng tuần) qua IStoreService/GetAppList — cần STEAM_API_KEY.

Ghi data/applist.json: [{"appid": ..., "name": ..., "last_modified": ...}, ...]
"""
import argparse
import json
import logging
import time
from pathlib import Path

import requests

from crawlers.config import STEAM_API_KEY, STEAM_WEB_API_BASE
from crawlers.common.public_datasets import replace_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SteamAppListCrawler")
URL = f"{STEAM_WEB_API_BASE}/IStoreService/GetAppList/v1/"


def fetch_all(max_pages: int = 100, page_size: int = 50000, interval: float = 1.0):
    if not STEAM_API_KEY:
        raise SystemExit("STEAM_API_KEY chưa được đặt trong .env")
    apps, last_appid = [], 0
    for _ in range(max_pages):
        params = {"key": STEAM_API_KEY, "max_results": page_size, "last_appid": last_appid,
                  "include_games": "true", "include_dlc": "false", "include_software": "false"}
        for attempt in range(5):
            resp = requests.get(URL, params=params, timeout=60)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 30 * (2 ** attempt)
                logger.warning("HTTP %s, retry in %ss", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError("GetAppList failed after retries")
        body = resp.json().get("response", {})
        apps.extend(body.get("apps", []))
        logger.info("Fetched %s apps so far", len(apps))
        if not body.get("have_more_results"):
            break
        last_appid = body["last_appid"]
        time.sleep(interval)
    return apps


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/applist.json")
    args = parser.parse_args()
    apps = fetch_all()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name("." + out.name + ".tmp")
    tmp.write_text(json.dumps(apps, ensure_ascii=False), encoding="utf-8")
    replace_file(tmp, out)
    logger.info("Saved %s apps to %s", len(apps), out)
