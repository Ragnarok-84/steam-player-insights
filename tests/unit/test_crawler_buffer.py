"""Unit tests for the memory buffer, batch flushing, and deferred review state updates."""
import gzip
import json
import time
from pathlib import Path

import pytest

from crawlers.common.jsonl_producer import JsonlProducer
from crawlers.common.rate_limiter import RateLimiter
from crawlers.common.review_state import ReviewStateStore, new_state
from crawlers import steam_reviews_crawler as reviews


def read_gz_lines(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        return [json.loads(line) for line in f]


def test_buffer_flushes_when_reaching_max_records(tmp_path):
    """Kịch bản 1: Buffer tự động flush khi chạm mốc 500 bản ghi."""
    producer = JsonlProducer(output_dir=str(tmp_path / "raw"), flush_max_records=500, flush_interval_sec=60)
    topic = "steam.reviews.raw"

    # Gửi 499 bản ghi -> chưa đạt 500 -> chưa ghi file ra đĩa
    for i in range(499):
        producer.send(topic, f"key_{i}", {"recommendationid": f"rec_{i}", "content": "review"})

    raw_files = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files) == 0, "Buffer chưa chạm ngưỡng 500 nên không được ghi ra file"
    assert producer.has_pending(topic)

    # Gửi bản ghi thứ 500 -> đạt ngưỡng -> tự động xả (flush) xuống đĩa
    producer.send(topic, "key_499", {"recommendationid": "rec_499", "content": "review"})

    raw_files = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files) == 1, "Buffer phải tự động flush ra đúng 1 file khi đủ 500 bản ghi"
    assert not producer.has_pending(topic), "Buffer phải rỗng sau khi flush"

    # Kiểm tra nội dung file nén
    records = read_gz_lines(raw_files[0])
    assert len(records) == 500
    assert records[0]["recommendationid"] == "rec_0"
    assert records[-1]["recommendationid"] == "rec_499"

    # Gửi thêm 500 bản ghi tiếp theo -> phải ghi ra file mới, không ghi đè file cũ
    for i in range(500, 1000):
        producer.send(topic, f"key_{i}", {"recommendationid": f"rec_{i}", "content": "review"})

    raw_files_after = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files_after) == 2, "Lô 500 bản ghi tiếp theo phải tạo file mới, không ghi đè"
    total_records = sum(len(read_gz_lines(f)) for f in raw_files_after)
    assert total_records == 1000


def test_buffer_flushes_when_exceeding_interval(monkeypatch, tmp_path):
    """Kịch bản 2: Buffer tự động flush khi quá 60 giây."""
    fake_time = [1_700_000_000.0]

    def current_time():
        return fake_time[0]

    monkeypatch.setattr(time, "time", current_time)

    producer = JsonlProducer(output_dir=str(tmp_path / "raw"), flush_max_records=500, flush_interval_sec=60.0)
    topic = "steam.players.ccu"

    # Gửi 10 bản ghi tại t0
    for i in range(10):
        producer.send(topic, f"key_{i}", {"appid": 100 + i, "player_count": 50})

    raw_files = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files) == 0, "Chưa quá 60s và chưa đủ 500 records -> không flush"

    # Tăng thời gian thêm 30s (chưa quá 60s)
    fake_time[0] += 30.0
    producer.flush()  # flush() không force -> kiểm tra điều kiện
    assert len(list((tmp_path / "raw").rglob("*.jsonl.gz"))) == 0

    # Tăng thời gian thêm 31s (tổng cộng 61s > 60s)
    fake_time[0] += 31.0
    # Gửi thêm 1 bản ghi để kích hoạt kiểm tra flush (hoặc gọi producer.flush())
    producer.send(topic, "key_extra", {"appid": 999, "player_count": 10})

    raw_files_after = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files_after) == 1, "Khi quá 60s, buffer phải tự động flush"

    records = read_gz_lines(raw_files_after[0])
    assert len(records) == 11
    assert not producer.has_pending(topic)


def test_buffer_flushes_all_on_close(tmp_path):
    """Kịch bản 3: Buffer xả hết dữ liệu còn lại khi đóng writer (close())."""
    producer = JsonlProducer(output_dir=str(tmp_path / "raw"), flush_max_records=500, flush_interval_sec=60.0)

    # Gửi số lượng ít bản ghi vào nhiều nguồn khác nhau
    producer.send("steam.players.ccu", "10", {"appid": 10, "player_count": 100})
    producer.send("steam.players.ccu", "20", {"appid": 20, "player_count": 200})

    producer.send("steam.games.meta", "10:meta", {"appid": 10, "name": "Game 10"})
    producer.send("steam.games.meta", "20:meta", {"appid": 20, "name": "Game 20"})
    producer.send("steam.games.meta", "30:meta", {"appid": 30, "name": "Game 30"})

    producer.send("steam.reviews.raw", "10", {"recommendationid": "r1", "appid": 10})

    # Trước khi close, chưa đạt 500 bản ghi và chưa quá 60s
    assert len(list((tmp_path / "raw").rglob("*.jsonl.gz"))) == 0

    # Đóng producer -> bắt buộc flush toàn bộ các buffer
    producer.close()

    ccu_files = list((tmp_path / "raw" / "steam_players_ccu").rglob("*.jsonl.gz"))
    meta_files = list((tmp_path / "raw" / "steam_games_meta").rglob("*.jsonl.gz"))
    review_files = list((tmp_path / "raw" / "steam_reviews_raw").rglob("*.jsonl.gz"))

    assert len(ccu_files) == 1
    assert len(meta_files) == 1
    assert len(review_files) == 1

    assert len(read_gz_lines(ccu_files[0])) == 2
    assert len(read_gz_lines(meta_files[0])) == 3
    assert len(read_gz_lines(review_files[0])) == 1

    # Không còn file tạm .tmp nào sót lại
    tmp_files = list((tmp_path / "raw").rglob("*.tmp"))
    assert len(tmp_files) == 0, "Không được để lại file .tmp sau khi ghi nguyên tử hoàn tất"


def test_review_state_not_saved_until_flush(monkeypatch, tmp_path):
    """Kịch bản 4: State review CHƯA được lưu xuống đĩa khi dữ liệu còn trong buffer,
    và CHỈ được lưu ngay sau khi flush thành công.
    """
    producer = JsonlProducer(output_dir=str(tmp_path / "raw"), flush_max_records=500, flush_interval_sec=60.0)
    store = ReviewStateStore(str(tmp_path / "state"))
    limiter = RateLimiter(0)

    appid = 730
    state_file = tmp_path / "state" / f"reviews_{appid}.json"

    # Giả lập Steam API trả về 5 reviews (< 500 records)
    fake_reviews = [
        {"recommendationid": f"rev_{i}", "review": "good", "timestamp_created": 1000 + i,
         "author": {"steamid": f"user_{i}"}}
        for i in range(5)
    ]

    def fake_fetch_page(app, cursor, rate_limiter, max_retries):
        if cursor == "*":
            return fake_reviews, "cursor_next"
        return [], cursor

    monkeypatch.setattr(reviews, "fetch_page", fake_fetch_page)

    # Chạy crawl review cho app 730
    ok = reviews.crawl_reviews_for_app(appid, producer, limiter, store, max_pages=1)
    assert ok

    # Dữ liệu 5 reviews đang nằm trong RAM buffer
    raw_files_before = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files_before) == 0, "Reviews vẫn đang nằm trong RAM buffer"
    assert producer.has_pending("steam.reviews.raw")

    # ĐIỀU KIỆN TIÊN QUYẾT: File state CHƯA ĐƯỢC LƯU XUỐNG ĐĨA!
    assert not state_file.exists(), "State tuyệt đối không được ghi xuống đĩa khi dữ liệu còn trong RAM buffer"
    assert store.has_deferred(appid), "State phải nằm trong hàng đợi chờ deferred"

    # Gọi flush_all() -> ghi file .jsonl.gz thành công
    producer.flush_all()

    # Sau khi flush: File .jsonl.gz đã tồn tại
    raw_files_after = list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert len(raw_files_after) == 1
    assert len(read_gz_lines(raw_files_after[0])) == 5

    # VÀ NGAY SAU KHI FLUSH THÀNH CÔNG: File state ĐÃ ĐƯỢC LƯU XUỐNG ĐĨA!
    assert state_file.exists(), "State phải được lưu xuống đĩa ngay sau khi flush hoàn thành"
    saved_state = store.load(appid)
    assert saved_state["reviews_emitted"] == 5
    assert saved_state["newest_ts"] == 1004
    assert not store.has_deferred(appid)


def test_dt_partitioning_by_source_and_date(tmp_path):
    """Kiểm tra phân nhóm buffer theo (topic_dir, dt) đúng thư mục nguồn và phân vùng ngày UTC."""
    producer = JsonlProducer(output_dir=str(tmp_path / "raw"))
    producer.send("steam.reviews.raw", "1", {"recommendationid": "1"})
    producer.send("steam.players.ccu", "1", {"appid": 1, "player_count": 10})
    producer.send("steam.games.meta", "1", {"appid": 1, "name": "Game"})
    producer.close()

    # Kiểm tra cả 3 thư mục nguồn được tạo
    assert (tmp_path / "raw" / "steam_reviews_raw").exists()
    assert (tmp_path / "raw" / "steam_players_ccu").exists()
    assert (tmp_path / "raw" / "steam_games_meta").exists()

    # Kiểm tra phân vùng dt=YYYY-MM-DD
    dt_dirs = list((tmp_path / "raw").glob("*/dt=*"))
    assert len(dt_dirs) == 3
    for d in dt_dirs:
        assert d.name.startswith("dt=")


def test_env_and_cli_configurations(monkeypatch, tmp_path):
    """Kiểm tra cấu hình qua biến môi trường và tham số CLI."""
    monkeypatch.setenv("CRAWLER_FLUSH_MAX_RECORDS", "250")
    monkeypatch.setenv("CRAWLER_FLUSH_INTERVAL_SEC", "45")

    p = JsonlProducer(output_dir=str(tmp_path / "raw"))
    assert p.flush_max_records == 250
    assert p.flush_interval_sec == 45.0

    p_cli = JsonlProducer(output_dir=str(tmp_path / "raw"), flush_max_records=100, flush_interval_sec=20.0)
    assert p_cli.flush_max_records == 100
    assert p_cli.flush_interval_sec == 20.0

