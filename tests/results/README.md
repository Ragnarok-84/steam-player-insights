# Mẫu kết quả đo chung (so sánh 100 / 500 / 1000 game)

Chốt ngày 13/11/2026. Mọi số đo đưa vào báo cáo và slide phải nằm trong `tests/results/common/` theo mẫu này. Script benchmark cũ (`sc1_sc2_*.csv`, `sc3_*.csv`, `sc4_*.csv` ở thư mục này) vẫn giữ file chi tiết riêng, nhưng **phải ghi thêm** các số chính vào mẫu chung bằng `tests/results_log.py`.

## Cột

Một dòng là **một số đo**. Một lần chạy ghi nhiều metric thì có nhiều dòng cùng `run_id`.

| Cột | Ý nghĩa | Giá trị hợp lệ |
| --- | --- | --- |
| `run_id` | Mã một lần chạy, tự sinh | `20261120-093000-B-batch-b2` |
| `measured_at` | Thời điểm ghi, UTC, tự sinh | ISO 8601 |
| `member` | Người đo | `A`, `B`, `C`, `D`, `lead` |
| `experiment` | Nhóm thí nghiệm | `crawl`, `storage`, `batch`, `streaming`, `es_query`, `fault`, `ml`, `resource` |
| `scale` | Số game | `100`, `500`, `1000` |
| `env` | Môi trường | `local`, `k3s` |
| `dataset` | Tên gói dữ liệu đã dùng | ví dụ `steam_scale_1000g_20261120-2300` |
| `job` | Job/bước được đo | `b2`…`b9`, `s1`…`s3`, `ft1`…`ft5`, `d1`…`d4`, nguồn crawl… |
| `executors` | Số executor Spark; để trống nếu không áp dụng | số nguyên |
| `repeat` | Lần lặp | 1, 2, 3… |
| `metric` | Tên số đo | xem bảng dưới |
| `value` | Giá trị | số |
| `unit` | Đơn vị, tự suy từ `metric` | xem bảng dưới |
| `status` | Kết quả lần chạy | `ok`, `fail`, `timeout` |
| `git_commit` | Commit lúc đo, tự sinh | |
| `notes` | Ghi chú tự do | cấu hình đặc biệt, sự cố |

## Metric dùng chung

| Nhóm | Metric (đơn vị) |
| --- | --- |
| Thời gian | `duration_s`, `startup_s`, `recovery_s`, `green_wait_s`, `train_s`, `predict_s` (s) |
| Khối lượng | `records`, `requests`, `games_covered`, `files`, `doc_count` (count), `bytes` (bytes) |
| Tốc độ | `requests_per_s`, `qps` (1/s), `throughput_rps` (rows/s), `speedup` (x) |
| Độ trễ | `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms` (ms) |
| Lỗi | `errors`, `http_429`, `data_loss`, `alerts` (count), `consumer_lag` (messages) |
| Tài nguyên, chi phí | `peak_mem_mb` (MB), `cpu_peak_pct` (%), `vm_hours` (h), `cost_usd` (USD) |
| Chất lượng | `accuracy`, `f1`, `auc`, `silhouette` (ratio, 0–1) |

Cần metric mới: thêm vào `METRICS` trong `tests/results_log.py` qua PR, không tự đặt tên trong file CSV.

## Ai đo gì

| Người | `experiment` | Metric tối thiểu |
| --- | --- | --- |
| A | `crawl`, `storage` | `duration_s`, `requests`, `requests_per_s`, `records`, `errors`, `http_429`, `games_covered`; `bytes`, `files` của raw |
| B | `batch`, `storage`, `ml` | `duration_s`, `startup_s`, `peak_mem_mb`, `cpu_peak_pct` theo job B2–B8; `bytes`, `files` của Parquet; B9: `train_s`, `predict_s`, `accuracy`, `f1`, `auc` (job `b9` và `vader` để so sánh) |
| C | `streaming`, `es_query` | `throughput_rps`, `latency_p50_ms`, `latency_p95_ms`, `consumer_lag` theo S1–S3; `qps`, `latency_p95_ms`, `doc_count`, `bytes` theo panel D1–D4 |
| D | `fault`, `resource` | `recovery_s`, `data_loss` theo FT1–FT5; `vm_hours`, `cost_usd` theo mức |
| lead | `batch` (job `b6`, `b8`), `streaming` | `silhouette`, `alerts`; ghi thêm nhận xét độ ổn định trending vào `notes` |

## Quy tắc

1. Mỗi cấu hình (scale × executors × job) chạy **ít nhất 3 lần** (`repeat` 1–3). Báo cáo dùng trung vị.
2. Chạy hỏng vẫn ghi, với `status=fail` và lý do trong `notes`. Không xoá dòng hỏng.
3. Chỉ so sánh các dòng có cùng `dataset` và `env`.
4. Chạy `python tests/results_log.py check` trước khi mở PR. File CSV trong thư mục này được commit lên git.
5. Tên file tự sinh: `common/<experiment>_<scale>_<env>_<ngày>_<member>.csv`. Không đổi tên, không sửa tay.

## Ví dụ

```bash
# A: nhập báo cáo crawl
python tests/results_log.py import-crawl --member A --scale 100 data/raw/collection-worker-0.run1.json

# C: ghi một lần đo streaming
python tests/results_log.py add --member C --experiment streaming --scale 500 --env local \
    --job s1 --repeat 1 --dataset steam_scale_1000g_20261120-2300 throughput_rps=1180 latency_p95_ms=2520

# Trưởng nhóm: gộp cho notebook
python tests/results_log.py merge /tmp/all_results.csv
```

```python
# B: ghi từ trong script benchmark
from tests.results_log import record
record(member="B", experiment="batch", scale=1000, env="k3s", job="b2", executors=4, repeat=2,
       dataset="steam_scale_1000g_20261120-2300", metrics={"duration_s": 63.4, "startup_s": 18.1})
```

Đọc trong notebook:

```python
import pandas as pd
df = pd.read_csv("all_results.csv")
summary = (df[df.status == "ok"]
           .groupby(["experiment", "job", "env", "executors", "metric", "scale"], dropna=False)["value"]
           .median().unstack("scale"))   # mỗi mức 100 / 500 / 1000 là một cột
```
