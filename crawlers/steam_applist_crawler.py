"""Fetch the game catalog directly from Steam using STEAM_API_KEY from .env."""
import argparse
import json
import logging
import time

import requests

from crawlers.config import STEAM_API_KEY, STEAM_WEB_API_BASE, CRAWLER_RATE_LIMIT_DELAY
from crawlers.common.files import write_json

logger = logging.getLogger(__name__)
URL = f"{STEAM_WEB_API_BASE}/IStoreService/GetAppList/v1/"


def fetch_all(max_pages=100, page_size=50000, interval=CRAWLER_RATE_LIMIT_DELAY):
    if not STEAM_API_KEY:
        raise ValueError("STEAM_API_KEY chưa được đặt trong .env")
    if max_pages < 1 or not 1 <= page_size <= 50000 or interval < 0:
        raise ValueError("Invalid catalog page limit, page size or interval")
    apps, last_appid = {}, 0
    for _ in range(max_pages):
        params = {"key": STEAM_API_KEY, "input_json": json.dumps({
            "max_results": page_size, "last_appid": last_appid,
            "include_games": True, "include_dlc": False, "include_software": False,
            "include_videos": False, "include_hardware": False})}
        for attempt in range(5):
            try:
                resp = requests.get(URL, params=params, timeout=60)
            except requests.RequestException:
                raise RuntimeError("Steam catalog request failed; check network configuration") from None
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < 4:
                    time.sleep(30 * 2**attempt)
                continue
            if resp.status_code != 200:
                # Requests exceptions include the URL and key. Never expose them.
                raise RuntimeError(f"Steam catalog returned HTTP {resp.status_code}")
            break
        else:
            raise RuntimeError("Steam catalog failed after retries")
        body = resp.json().get("response")
        if not isinstance(body, dict) or not isinstance(body.get("apps"), list) or not body['apps']:
            raise ValueError("Invalid or empty Steam catalog response")
        for app in body['apps']:
            appid = int(app['appid'])
            if appid <= 0:
                raise ValueError("Steam catalog returned invalid appid")
            apps[appid] = {**app, 'appid': appid}
        logger.info("Fetched %s Steam games", len(apps))
        if not body.get("have_more_results"):
            return sorted(apps.values(), key=lambda a: a['appid'])
        next_appid = int(body['last_appid'])
        if next_appid <= last_appid:
            raise ValueError("Steam catalog pagination did not advance")
        last_appid = next_appid
        time.sleep(interval)
    raise RuntimeError("Steam catalog is incomplete: max_pages reached; no targets written")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='data/applist.json')
    args = parser.parse_args()
    write_json(args.output, fetch_all())


if __name__ == '__main__':
    main()
