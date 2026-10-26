"""Mock Steam Web API cho FT6: trả HTTP 429 cho N request đầu (hoặc trong khoảng thời gian), sau đó trả CCU hợp lệ.

  python tests/fault_tolerance/mock_steam_api.py --port 8099 --fail-first 5
  STEAM_WEB_API_BASE=http://localhost:8099 CRAWLER_OUTPUT=jsonl python -m crawlers.steam_ccu_crawler
"""
import argparse
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_handler(fail_first: int, stats: dict, lock: threading.Lock):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - tên do http.server quy định
            with lock:
                stats["requests"] += 1
                failing = stats["requests"] <= fail_first
                stats["429" if failing else "200"] += 1
            if failing:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.end_headers()
                return
            body = json.dumps({"response": {"result": 1, "player_count": random.randint(1000, 5000)}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def serve(port: int, fail_first: int):
    stats, lock = {"requests": 0, "429": 0, "200": 0}, threading.Lock()
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(fail_first, stats, lock))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, stats


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--port", type=int, default=8099)
    p.add_argument("--fail-first", type=int, default=5)
    a = p.parse_args()
    srv, st = serve(a.port, a.fail_first)
    print(f"Mock Steam API on :{a.port}, first {a.fail_first} requests -> 429. Ctrl+C to stop.")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print(st)
