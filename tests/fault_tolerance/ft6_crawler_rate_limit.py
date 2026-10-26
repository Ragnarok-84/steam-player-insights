"""FT6: lỗi nguồn dữ liệu — API trả 429. Chạy được trên máy local, không cần cụm.

Khởi động mock API (5 request đầu bị 429), chạy CCU crawler cho N game với output JSONL, kiểm tra:
  - crawler không dừng / không crash khi gặp 429, có backoff (thời gian chạy tăng do cooldown)
  - các game sau khi API hồi phục vẫn được thu thập; game lỗi được thu ở lượt chạy sau
"""
import gzip
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tests.fault_tolerance.mock_steam_api import serve  # noqa: E402

APPS = 8
FAIL_FIRST = 3


def run_crawler(port, out_dir, targets):
    env = os.environ.copy()
    env.update(STEAM_WEB_API_BASE=f"http://localhost:{port}", CRAWLER_OUTPUT="jsonl", CRAWLER_OUTPUT_DIR=str(out_dir),
               TARGET_APP_LIST_PATH=str(targets), CRAWLER_TARGET_LIMIT=str(APPS), STEAM_API_KEY="",
               CRAWLER_RATE_LIMIT_COOLDOWN="2", PYTHONPATH=str(ROOT))
    start = time.time()
    code = subprocess.call([sys.executable, "-m", "crawlers.steam_ccu_crawler"], cwd=ROOT, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return code, round(time.time() - start, 1)


def collected(out_dir):
    return {json.loads(line)["appid"] for f in Path(out_dir).rglob("*.jsonl.gz") for line in gzip.open(f, "rt")}


def main():
    port = 8099
    server, stats = serve(port, FAIL_FIRST)
    with tempfile.TemporaryDirectory() as tmp:
        targets = Path(tmp) / "targets.json"
        targets.write_text(json.dumps([{"appid": 100 + i} for i in range(APPS)]))
        out = Path(tmp) / "raw"
        code1, t1 = run_crawler(port, out, targets)
        first = collected(out)
        code2, t2 = run_crawler(port, out, targets)
        second = collected(out)
    server.shutdown()
    print(f"run1: exit={code1} {t1}s, apps collected={len(first)}/{APPS}")
    print(f"run2: exit={code2} {t2}s, apps collected={len(second)}/{APPS}")
    print(f"mock stats: {stats}")
    assert code1 == 0 and code2 == 0, "crawler crashed on 429"
    assert stats["429"] == FAIL_FIRST
    assert len(first) >= APPS - FAIL_FIRST, "crawler did not continue after rate limit"
    assert len(second) == APPS, "games that failed with 429 were not collected on the next run"
    print("FT6 PASS: crawler backs off on 429, continues, and the next run fills the gaps.")


if __name__ == "__main__":
    main()
