# Benchmark speed layer (SC3) và Elasticsearch (SC4) theo quy mô 100 / 500 / 1000 game (local, 23/11/2026)

Người đo: Mai Kiên (thành viên C). Giai đoạn 2 của phần so sánh quy mô. Đây là số đo **một máy**; giai đoạn 3 đo lại trên cụm k3s.

## 1. Cách đo

| Mục | Giá trị |
| --- | --- |
| Dữ liệu | Gói `steam_1000g_20261118` của Hải Long, cắt bằng `tests/scalability/make_scale_subsets.py` thành 3 tập lồng nhau. Số bản ghi trùng khớp `tests/results/scale_subsets_20261118.json` mà Mạnh Hùng dùng cho batch (192.239 / 619.514 / 932.293 review) |
| Máy | Windows 11 + Docker Desktop (VM Linux 10 CPU, 11,5 GB RAM). Kafka 3.9.1 (1 broker, 1 partition/topic), Elasticsearch 8.19.3 (1 node, heap 512 MB), Kibana 8.19.3, theo `compose.demo.yaml` |
| Spark | Image `steam-spark:dev` (Spark 3.5.7), mount code commit `af8377d` (SC4) và `1d45953` (SC3 đo lại lần 2 ngày 24/11, xem mục 2). Batch `local[4]`, driver 5 GB; streaming `local[2]`, driver 2 GB, trigger 10 s |
| Trình tự mỗi quy mô | SC4: 1) xoá và tạo lại index ES; 2) `pipeline.py batch --es` trên `data/scale/raw_N` (nạp D1/D2/D4 và baseline B7 cho S2/S3); 3) SC3: S1, S2, S3 × 3 lượt; 4) SC4 × 3 lượt; 5) chụp D1–D4. SC3 đo lại lần 2: bước 1–3 cho từng mức, thứ tự 1000 → 100 → 500 game |
| SC3 | `sc3_streaming_load_test.py`: phát lại bản ghi của tập N vào Kafka (đọc bằng `crawlers/replay_jsonl_to_kafka.py`, dời timestamp về hiện tại), 2 bậc tải 1.000 và 5.000 rec/s, mỗi bậc 60 s. S1/S2 dùng 3 producer song song (`--producers 3`), S3 một producer. Job ghi ES thật. Producer chạy trong container cùng mạng Docker, cùng đồng hồ với job |
| SC4 | `sc4_es_query_load.py`: chạy đúng truy vấn ES của từng panel D1–D4 (lấy từ `dashboards/setup_kibana.py`), 8 luồng đồng thời, 60 s mỗi dashboard. D2 lọc một appid ngẫu nhiên mỗi truy vấn |
| Lặp | 3 lần mỗi cấu hình, báo cáo **trung vị**. 27/27 lượt SC3 (đo lại lần 2) và 9/9 lượt SC4 thành công, 0 lỗi truy vấn |

Kết quả: mẫu chung `tests/results/common/es_query_{N}_local_20261123_C.csv` (SC4) và `streaming_{N}_local_20261124_C.csv` (SC3, dataset `steam_1000g_20261118_rerun20261124`); chi tiết từng bậc / từng panel `tests/results/sc3_{job}_{N}g_local_r{1,2,3}_*.csv`, `tests/results/sc4_{N}g_local_r{1,2,3}_*.csv`; ảnh `docs/screenshots/scale/d{1..4}_{N}g.png`.

**Độ trễ end-to-end** (`processing/common/spark_utils.py`, `with_e2e_latency`): từ lúc message vào Kafka (CreateTime) tới lúc `foreachBatch` ghi xong ES. Mỗi dòng cửa sổ (appid × cửa sổ) cập nhật trong micro-batch là một mẫu, lấy sự kiện mới nhất của appid đó.

## 2. SC3: speed layer

Trung vị 3 lượt; mỗi ô ghi `100 / 500 / 1000 game`.

**Ba lượt đo SC3.** Bảng dưới chỉ dùng **lượt đo lại lần 2** (24/11, dataset `steam_1000g_20261118_rerun20261124`). Hai lượt trước vẫn nằm trong CSV, không sửa tay, nhưng không dùng:

- *Lượt đầu* (23/11, commit `af8377d`, dataset `steam_1000g_20261118`) lấy lag từ `maxOffsetsBehindLatest` của Spark. Stream không đặt `maxOffsetsPerTrigger` nên mỗi micro-batch đọc tới offset mới nhất, chỉ số này luôn bằng 0 và `keeps_up` luôn đúng.
- *Lượt đo lại lần 1* (23–24/11, commit `071b64f`, cùng dataset `steam_1000g_20261118`) dùng cách đo lag mới, nhưng các lượt 1000 game chạy khi máy rút sạc và ngủ. S2 r1 bậc 1.000 có p95 535 s, p99 537 s. S2 r3 có p99 589 s và chỉ gửi 59.000/60.000 bản ghi. S3 r2 có p95 234 s và chỉ gửi 51.000/60.000 bản ghi, tức chưa đạt tải. Bậc 5.000 của S1/S2 ở lượt này chạy một producer, chỉ đạt 2.500–3.917 rec/s.

Lượt đo lại lần 2: máy cắm sạc, chặn ngủ suốt lượt đo, ES và lake nạp lại cho từng mức. Mọi lượt dài đúng 3,4 phút, gửi đủ tải (60.000 / 300.000 bản ghi mỗi bậc), p95 lớn nhất 11,1 s, 0 lỗi trong log job.

*Lag lớn nhất*: lấy mẫu offset cuối của topic mỗi giây, trừ offset mà query đã xử lý xong, lấy đỉnh trong bậc tải. *Lag còn lại* (`final_lag`, trong `notes`): phần tồn sau khi ngừng gửi 15 s.

| Job | Tải mục tiêu | Tải thật (rec/s) | Thông lượng xử lý (rows/s) | e2e p50 (s) | e2e p95 (s) | Lag lớn nhất (msg) | Lag còn lại |
| --- | --- | --- | --- | --- | --- | --- | --- |
| S1 | 1.000 | 1.000 / 1.000 / 1.000 | 6.619 / 7.036 / 6.837 | 5,6 / 5,4 / 5,8 | 10,4 / 10,6 / 10,7 | 11.996 / 12.000 / 11.635 | 0 / 0 / 0 |
| S1 | 5.000 | 5.000 / 5.000 / 5.000 | 21.881 / 23.393 / 20.663 | 4,9 / 5,0 / 5,4 | 10,6 / 10,6 / 10,7 | 65.847 / 65.738 / 61.447 | 0 / 0 / 0 |
| S2 | 1.000 | 1.000 / 1.000 / 1.000 | 7.234 / 6.814 / 7.513 | 5,5 / 5,5 / 5,3 | 10,9 / 10,3 / 10,6 | 12.373 / 12.000 / 12.000 | 0 / 0 / 0 |
| S2 | 5.000 | 5.000 / 5.000 / 5.000 | 25.060 / 23.418 / 24.401 | 4,7 / 4,9 / 4,9 | 10,6 / 10,6 / 10,7 | 63.022 / 59.983 / 59.009 | 0 / 0 / 0 |
| S3 | 1.000 | 1.000 / 1.000 / 1.000 | 8.995 / 8.815 / 7.879 | 1,6 / 1,5 / 1,5 | 5,7 / 7,2 / 6,3 | 11.000 / 12.000 / 11.000 | 0 / 0 / 0 |
| S3 | 5.000 | 5.000 / 5.000 / 5.000 | 45.266 / 39.448 / 37.638 | 1,3 / 1,2 / 1,2 | 7,5 / 6,1 / 8,3 | 55.000 / 56.585 / 55.248 | 0 / 0 / 0 |

- *Tải thật* = số bản ghi đã gửi / 60 s (metric `records`). *Thông lượng xử lý* = `processedRowsPerSecond` trung bình của micro-batch, tức tốc độ khi đang xử lý. Số này cao hơn tải vì mỗi micro-batch chỉ chạy vài giây trong chu kỳ 10 s.
- p99 (trung vị 3 lượt): S1/S2 10,7–12,0 s, S3 5,7–8,3 s ở mọi mức và mọi bậc.

## 3. SC4: Elasticsearch theo dashboard

Trung vị 3 lượt; `100 / 500 / 1000 game`. Số doc và dung lượng là tổng các index mà dashboard đọc (primary, không replica).

| Dashboard | qps | p50 (ms) | p95 (ms) | p99 (ms) | Số doc | Dung lượng index (MB) |
| --- | --- | --- | --- | --- | --- | --- |
| D1 Tổng quan | 435 / 174 / 102 | 7,8 / 15,4 / 92,4 | 71,0 / 100,1 / 189,8 | 89,2 / 109,3 / 204,0 | 63.135 / 227.864 / 372.927 | 5,5 / 23,4 / 39,7 |
| D2 Chi tiết game | 402 / 356 / 331 | 8,5 / 9,7 / 11,9 | 75,1 / 80,5 / 81,7 | 93,1 / 98,8 / 102,2 | 130.685 / 536.946 / 895.851 | 37,4 / 149,2 / 238,5 |
| D3 Thời gian thực | 1.061 / 1.020 / 903 | 5,5 / 6,1 / 7,0 | 22,5 / 17,4 / 19,9 | 47,5 / 50,9 / 52,7 | 1.900 / 7.992 / 12.786 | 0,3 / 0,9 / 1,3 |
| D4 Cảnh báo & xu hướng | 329 / 261 / 202 | 14,9 / 20,9 / 26,8 | 76,7 / 86,7 / 103,5 | 103,7 / 116,9 / 163,6 | 34.792 / 167.877 / 297.234 | 8,9 / 44,1 / 76,4 |

Không có truy vấn lỗi ở mọi quy mô. Lượt 1 ở 100 game chậm hơn hai lượt sau (ES còn nguội, D1 p95 95,6 ms so với 69–71 ms), trung vị đã loại ảnh hưởng này.

## 4. Nhận xét

1. **Speed layer theo kịp ở cả 3 quy mô**: lag lớn nhất ≈ tải × (trigger 10 s + thời gian batch). Ở 1.000 rec/s là 11.000–12.400 msg; ở 5.000 rec/s là 55.000–57.000 msg với S3 và 59.000–66.000 msg với S1/S2 (batch review dài hơn). Đây là lag tự nhiên của micro-batch, không phải tồn đọng: lag còn lại = 0 ở cả 27 lượt, `keeps_up` đúng ở mọi bậc. Thời gian micro-batch vẫn ngắn hơn trigger, nên ở tải này chưa tìm được điểm bão hoà.
2. **Độ trễ e2e bị chi phối bởi trigger, không phải số game**: p95 ≈ trigger 10 s + thời gian batch 1–1,5 s ở mọi quy mô. Muốn giảm độ trễ thì giảm trigger, không cần thêm tài nguyên.
3. **S3 có p50 thấp hơn S1/S2 do thứ tự phát lại, không do job**: review trong file raw xếp liền theo appid (đoạn trung bình 94 review cùng appid), nên trong một chu kỳ 10 s phần lớn appid chỉ xuất hiện một lúc ngắn và sự kiện mới nhất của chúng trải đều trong chu kỳ (p50 ≈ 5 s). CCU xen kẽ appid, appid nào cũng có sự kiện ngay trước trigger (p50 ≈ 1,5 s). Dữ liệu thật từ crawler cũng đến theo từng trang review của một game, nên phân bố của S1/S2 sát thực tế hơn.
4. **Số game ảnh hưởng tới thông lượng của S3, gần như không ảnh hưởng S1/S2**: ở bậc 5.000 rec/s, từ 100 lên 1000 game, S3 giảm 45,3k → 37,6k rows/s (−17%). CCU xen kẽ appid nên mỗi micro-batch chạm vào nhiều khoá appid × cửa sổ hơn khi số game tăng, và nhiều dòng phải ghi ES hơn. S1/S2 dao động không theo xu hướng (S1 21,9k / 23,4k / 20,7k; S2 25,1k / 23,4k / 24,4k, tức ±7%). Lý do là review đến theo từng đoạn cùng appid, nên số khoá mỗi micro-batch ít phụ thuộc tổng số game. Lượt đầu ghi S1/S2 giảm 14–24% từ 100 lên 1000 game; mức đó chủ yếu do tải thật của một producer giảm dần theo số game (3.917 → 3.333 rec/s), không phải do job. Khi tải đủ 5.000 rec/s ở mọi mức, mức giảm này không còn.
5. **D1 là nút thắt của serving layer**: số doc tăng 5,9× thì qps giảm 4,3× (435 → 102) và p50 tăng 12× (7,8 → 92,4 ms). D1 tổng hợp trên toàn bộ `batch_game_daily`, `batch_language_stats`, `batch_segment_stats` trong 5 năm, và panel heatmap lấy tới 5.000 hit `batch_game_retention`. Bước nhảy lớn nhất ở 500 → 1000 game (p50 15 → 92 ms) cho thấy ES 1 node, heap 512 MB bắt đầu thiếu cache.
6. **D2 gần như không đổi** (p50 8,5 → 11,9 ms) dù index lớn 6,4×, vì mọi truy vấn lọc theo một appid: dữ liệu đọc tỷ lệ với số review của game đó, không phải tổng số game. D4 tăng vừa phải (p50 +80%). D3 nhỏ và nhanh ở mọi mức.
7. **Dung lượng index tăng gần tuyến tính theo review**: các index của D2 tăng 37 → 149 → 239 MB, tức 195 → 241 → 256 byte trên mỗi review raw (tăng nhẹ, một phần vì tỷ lệ review tiếng Anh được index vào `reviews_search` tăng từ 37,8% ở 100 game lên 40,0% ở 1000 game). `reviews_search` (chỉ review tiếng Anh) chiếm phần lớn: ở 1000 game là 373.097 doc, 182 MB.

## 5. Giới hạn

- **Một producer không đạt 5.000 rec/s với review**: một tiến trình Python (`kafka-python`) chỉ gửi được 3.300–3.900 rec/s. Vì vậy lượt đo lại lần 2 dùng `--producers 3` cho S1/S2 (K tiến trình song song, chia đều tải mục tiêu), và đạt đủ 5.000 rec/s ở mọi mức. Bậc 5.000 chưa phải điểm bão hoà; giai đoạn 3 cần bậc cao hơn (thêm producer) để tìm giới hạn.
- Kafka 1 broker, 1 partition, Spark `local[2]` trong Docker trên Windows: không đo được song song theo partition và chi phí mạng. Đo lại trên cụm (6 partition, nhiều executor).
- ES 1 node, heap 512 MB: số SC4 phản ánh cấu hình demo, không phải cấu hình cụm (thiết kế 3 node, 1 replica).
- SC4 chạy từ Windows host qua `127.0.0.1:9200`, 8 luồng Python: qps bị giới hạn một phần bởi client. So sánh giữa các quy mô vẫn công bằng vì cùng client.
- D3 và cảnh báo D4 trong ảnh là dữ liệu tải SC3 (timestamp dời về hiện tại), không phải hoạt động thật của Steam. D2 chụp với `appid:730`; panel CCU của D2 trống vì CCU mới có 1–2 snapshot mỗi game.
- Batch + ghi ES để nạp dữ liệu mất 265 / 361 / 472 s. Số này chỉ để tham khảo, không dùng thay cho benchmark batch của Mạnh Hùng (khác máy, có ghi ES).

## 6. Cách chạy lại

Bật dịch vụ (`MSYS_NO_PATHCONV=1` khi dùng Git Bash trên Windows):

```bash
docker compose -f compose.demo.yaml --profile streaming --profile dashboard up -d --wait kafka elasticsearch kibana
python tests/scalability/make_scale_subsets.py --raw data/incoming/data-1000g/raw \
    --targets data/incoming/data-1000g/target_apps.json --out data/scale --scales 100 500 1000 --dataset steam_1000g_20261118
```

Với mỗi N ∈ {100, 500, 1000}, trong container `steam-spark:dev` nối mạng `steam-insights-demo_default`, mount repo vào `/repo` và một volume lake riêng vào `/lake`:

```bash
# 1) ES sạch + batch
python3 -c "from storage.elasticsearch.setup_indices import init_indices; init_indices('infra/demo/elasticsearch-indices.json', recreate=True)"
python3 pipeline.py batch --es --raw data/scale/raw_N --lake /lake          # SPARK_MASTER=local[4] SPARK_DRIVER_MEMORY=5g
# 2) SC3, mỗi job s1|s2|s3 và lượt r=1..3: chạy job rồi bơm tải từ một container khác
python3 pipeline.py s1 --lake /lake --es -- --source kafka --brokers kafka:19092 --trigger "10 seconds" \
    --metrics-file /repo/data/bench/sc3_metrics.jsonl --latency-file /repo/data/bench/sc3_latency.jsonl
python3 tests/scalability/sc3_streaming_load_test.py --job s1 --scale N --repeat r --dataset steam_1000g_20261118 \
    --no-start-job --brokers kafka:19092 --es --rates 1000 5000 --step-seconds 60   # thêm --producers 3 để đủ 5.000 rec/s
```

Container không có `git`: truyền `-e GIT_COMMIT=$(git rev-parse --short HEAD)` để `tests/results_log.py` ghi đúng cột `git_commit`. Sau đó, từ máy host:

```bash
ELASTICSEARCH_HOSTS=http://127.0.0.1:9200 python tests/scalability/sc4_es_query_load.py --scale N --repeat r \
    --dataset steam_1000g_20261118 --concurrency 8 --seconds 60
```

Không chạy Kafka producer từ Windows host: Kafka quảng bá `localhost:9092`, Windows phân giải ra `::1` trước trong khi cổng chỉ mở trên `127.0.0.1`. Mỗi quy mô mất khoảng 50 phút; cả 3 mức khoảng 2,5 giờ.
