# Kế hoạch hoàn thành repo Steam Player Insights

Cập nhật: 2026-10-09. Đối chiếu code hiện có với tài liệu thiết kế `BigData.md` (mục 1–10).

## Trạng thái (2026-10-26)

| Giai đoạn | Trạng thái | Đã kiểm chứng |
| --- | --- | --- |
| 1 Batch | Xong (B1–B8) | Docker, dữ liệu mẫu; B1 qua Kafka không ghi trùng |
| 2 Speed + hợp nhất | Xong (S1–S3, `merge_views`) | Nguồn file và Kafka; cảnh báo ghi ES + Kafka |
| 3 Truy vấn / phân tích bổ sung | Xong (Hive, K-Means, review đáng ngờ, vòng phản hồi) | HMS 3.1.3 + PostgreSQL + Spark SQL |
| 4 Dashboard | Xong (Kibana D1–D4, Grafana D5) | Render trên Kibana 8.15, import Grafana 11 |
| 5 K8s | Xong (manifest + script) | kubeconform 32/32; **chưa chạy trên cụm thật** |
| 6 Kiểm thử | Xong (SC1–SC4, FT1–FT6, unit) | Unit 11/11, SC1–SC4 rút gọn, FT6; FT1–FT5 cần cụm |
| 7 Bàn giao | Code + tài liệu + notebook xong | Còn: chạy trên dữ liệu thật và cụm, báo cáo, slide, video |

Phần dưới là kế hoạch gốc, giữ để tham chiếu.

## 0. Đối chiếu thiết kế và code

| Mục thiết kế | Code hiện tại | Khoảng trống |
| --- | --- | --- |
| §2 Nguồn dữ liệu, F1 thu thập | Crawler reviews, appdetails, CCU, SteamSpy, Kaggle replay, backfill. Có chia worker và rate limiter | Chưa có CronJob cho appdetails/SteamSpy (hằng ngày) và AppList (hằng tuần). Chưa kiểm thử retry khi gặp 429 (FT6) |
| §5.5a Kafka topics | `kafka-topics.yaml` khớp: partition 6/3/3/1, RF 3, retention 7 ngày, meta compacted, `min.insync.replicas=2` | Không thiếu |
| §5.5b HDFS `/steam/raw/{reviews,ccu,meta}/dt=` | B1 ghi `/steam/raw/steam_reviews_raw`, có lỗi ghi `.text()` 3 cột | Sai đường dẫn. Lỗi làm `make batch` dừng ngay ở B1 |
| §5.5b Clean ccu, games | B2 chỉ làm sạch review | Thiếu clean CCU và clean games |
| §5.5c ES 5 index | Có mapping. B3, B5 có ghi ES | B4, B6 chưa ghi ES. S1 chưa ghi `realtime_game_metrics`. Chưa job nào ghi `alerts`. B3 dùng `es.mapping.id=appid` nên ngày sau ghi đè ngày trước |
| §6.1 B2: dedupe, lọc spam, gắn cờ ngôn ngữ | Dedupe, làm sạch, lọc spam đã có | Ghi `append` nên trùng giữa các lần chạy. Chưa gắn cờ review đáng ngờ (§6.3) |
| §6.1 B3: theo ngày/tuần/tháng, thể loại, nhà phát triển, khoảng giá; giờ chơi trung vị; cảm xúc trung bình | Chỉ theo `(appid, dt)` | Thiếu rollup tuần/tháng, join metadata, trung vị giờ chơi, cảm xúc trung bình. Kiểm tra đường dẫn CCU bằng `os.path.exists` |
| §6.1 B4: tỷ lệ review tiêu cực sớm (<2h) | Đã chia nhóm giờ chơi, có % tích cực | Thiếu chỉ số "bỏ game". Chưa ghi ES |
| §6.1 B5: khía cạnh, VADER, TF-IDF | Khía cạnh và VADER đã có | Thiếu TF-IDF từ khóa theo game (cần cho word cloud D2). ES lỗi bị nuốt |
| §6.1 B6: tăng trưởng tuần, xếp hạng, tác động trước/sau update hoặc sale | Tính `lag(7)` theo dòng | `lag(7)` không đúng 7 ngày. Thiếu `trending_score`, xếp hạng, phân tích trước/sau sự kiện |
| §6.2 S1: cửa sổ 60m/5m, watermark 10m | Logic đúng | Chỉ ghi console |
| §6.2 S2: vượt mean + 3σ của baseline 7 ngày, % tích cực giảm >20 điểm | Ngưỡng cứng ≥50 review và ≤35% | Chưa có baseline. Bug: `alert_type` chứa appid |
| §6.2 S3: CCU thay đổi >±50% so với cùng khung giờ các ngày trước | Chỉ tính avg/max, ghi console | Chưa phát hiện đột biến, chưa sinh cảnh báo |
| §5.3 bước 3: batch view thay thế phần realtime view tương ứng | Chưa có | Chưa có cơ chế hợp nhất batch view và realtime view |
| §5.3 bước 5: vòng phản hồi | Chưa có | Game trending chưa được thêm vào `target_apps` |
| §6.3 review đáng ngờ, K-Means | Chưa có | Thiếu job phân tích bổ sung |
| §5.4 K8s: Hive + PostgreSQL, Spark Operator, monitoring | Đã có Kafka, HDFS, ECK, RBAC, crawler CronJob | Thiếu `k8s/hive`, `k8s/monitoring`, SparkApplication. `deploy_all.sh` dùng `|| true` che lỗi |
| §5.4 Spark dynamic allocation 2–6 executor | Chưa có | Cấu hình trong SparkApplication |
| §7 D1–D5 | README mô tả | Thiếu Kibana ndjson và Grafana JSON |
| §8 SC1–SC4, FT1–FT6 | `sc1_sc2_batch_benchmark.py`, chaos script | Thiếu SC3, SC4 và kết quả đo |
| §8.3 bàn giao | README, deployment guide | Thiếu `architecture.md`, notebook, báo cáo, slide, video |

## Giai đoạn 1 — Batch layer đúng thiết kế (F2, F3)

1. **`spark_utils.py`:** dùng `HDFS_NAMENODE_URL` làm `spark.hadoop.fs.defaultFS`. Thêm `spark.jars.packages` cho `spark-sql-kafka-0-10_2.12:3.5.7` và `elasticsearch-spark-30_2.12` qua biến môi trường. Thêm helper `path_exists()` dùng Hadoop FileSystem.
2. **B1:**
   - Ánh xạ topic sang thư mục: `steam.reviews.raw` → `reviews`, `steam.players.ccu` → `ccu`, `steam.games.meta` → `meta`. Thêm `--topic all` để chạy cả ba.
   - Chỉ `select("json_value", "dt")` trước khi gọi `.text()`.
   - Lưu offset cuối vào `/steam/checkpoints/b1_offsets/` và dùng làm `startingOffsets` ở lần chạy sau.
3. **B2:**
   - Thêm `clean_ccu`: dedupe theo `(appid, timestamp)`, ghi `/steam/clean/ccu/dt=`.
   - Thêm `clean_games`: lấy snapshot mới nhất theo appid, gộp appdetails với SteamSpy (tags, owners), ghi `/steam/clean/games`.
   - Review: thêm cờ `is_suspicious` (giờ chơi ≈ 0, `num_reviews` = 1, nội dung trùng, `received_for_free`) và cờ `is_english`.
   - Ghi dynamic overwrite theo `dt`. Upsert theo `recommendationid` bằng cách đọc lại các partition bị ảnh hưởng.
4. **B3:**
   - Dùng `path_exists`. Luôn có `avg_ccu` và `peak_ccu` (null nếu không có dữ liệu CCU).
   - Thêm `median_playtime_hours` (`percentile_approx`) và `avg_sentiment` (lấy từ B5, nên chạy B5 trước B3 hoặc join sau).
   - Join `clean/games` để có `name, genres, developer, price_bucket`.
   - Thêm rollup tuần/tháng (cột `period`) và bảng theo thể loại/nhà phát triển/khoảng giá.
   - ES id = `appid_dt`.
5. **B4:** thêm `early_negative_ratio`: tỷ lệ review tiêu cực có giờ chơi <2h trên tổng review. Ghi ES (index phụ hoặc gộp vào `batch_game_daily`).
6. **B5:**
   - Thêm TF-IDF (`HashingTF` + `IDF` của Spark MLlib) trên review tiêu cực, lấy top-k từ khóa theo game, ghi `curated/keywords`.
   - Thêm `language`, `is_suspicious` vào `reviews_search`.
   - Lỗi ghi ES thì raise, không nuốt lỗi.
7. **B6:**
   - So sánh theo khoảng thời gian thật: `rangeBetween` trên số ngày, tổng 7 ngày gần nhất so với 7 ngày trước đó.
   - Tính `trending_score` = trọng số của z-score tăng trưởng review và CCU. Thêm `rank`.
   - Phân tích trước/sau sự kiện: % tích cực ±7 ngày quanh `discount > 0` hoặc ngày cập nhật lấy từ metadata.
   - Ghi ES vào index `trending` (thêm vào `indices_mapping.json`).
8. **Hive:** thêm bảng `clean_ccu`, `clean_games`, `keywords`. Chạy `MSCK REPAIR` sau batch.
9. **Makefile:** sắp thứ tự `b1 b2 b5 b3 b4 b6`.

**Xong khi:** từ dữ liệu Kaggle replay, `make batch` chạy sạch. Chạy lại lần 2 không làm tăng số dòng. Cả 5 index ES có dữ liệu.

## Giai đoạn 2 — Speed layer đúng thiết kế (F4, độ trễ ≤ 2 phút)

1. **S1:** dùng `foreachBatch` ghi `realtime_game_metrics`, id = `appid_windowstart_5m|60m`. Thêm cửa sổ 5 phút. Trigger 30 giây.
2. **S2:**
   - Baseline: trung bình và độ lệch chuẩn số review tiêu cực mỗi giờ, cùng % tích cực của 7 ngày, tính từ `clean_reviews`. Ghi `curated/review_baseline`, làm mới mỗi lần batch chạy.
   - Join stream–static. Điều kiện cảnh báo: `negative > mean + 3σ` VÀ `positive_ratio < baseline − 20`.
   - Sửa `alert_type = "REVIEW_BOMBING"`. Ghi Kafka `steam.alerts` và ES `alerts`.
3. **S3:**
   - Baseline: CCU trung bình theo `(appid, giờ trong ngày)` của các ngày trước, tính từ `clean_ccu`.
   - Cảnh báo khi |Δ| > 50%, loại `CCU_SPIKE` hoặc `CCU_DROP`.
   - Ghi `realtime_game_metrics` với CCU hiện tại, cùng `alerts` và Kafka.
4. **Hợp nhất Lambda (§5.3 bước 3):** sau B3, xóa các bản ghi realtime có `window_end` ≤ mốc batch (`delete_by_query`). Lưu mốc vào doc `serving_state` để dashboard biết lấy batch view tới đâu.
5. **Makefile:** thêm `s1 s2 s3`. Checkpoint đặt tại `/steam/checkpoints/{job}`.

**Xong khi:** đẩy một đợt review tiêu cực giả và một đợt CCU giảm 60% → cảnh báo xuất hiện trong `alerts` trong vòng 2 phút.

## Giai đoạn 3 — Truy vấn, phân tích bổ sung, vòng phản hồi (F5, §6.3)

1. Notebook hoặc script ví dụ chạy Spark SQL ad-hoc qua Hive Metastore. Ví dụ ES full-text query trên `reviews_search`.
2. **B7 (tùy chọn, khuyến khích):** K-Means (MLlib) trên vector [% tích cực, giờ chơi trung vị, giá, `early_negative_ratio`, tỷ lệ khía cạnh], ghi `curated/game_clusters`.
3. **Phân tích "đáng tiền":** xếp hạng theo giờ chơi trung vị / giá (gộp vào B4 hoặc B7).
4. **Vòng phản hồi:** script `crawlers/update_targets_from_trending.py` đọc top trending và thêm vào `data/target_apps.json`. Ngưỡng cảnh báo S2/S3 đọc từ biến môi trường hoặc ConfigMap.

## Giai đoạn 4 — Dashboard (F6, §7)

1. **Kibana D1:** review theo thời gian; % tích cực theo thể loại; top CCU; phân bố ngôn ngữ; heatmap giá × % tích cực.
2. **Kibana D2:** % tích cực theo tuần kèm annotation sự kiện; CCU; nhóm giờ chơi; khía cạnh; word cloud TF-IDF.
3. **Kibana D3:** review mới mỗi giờ; % tích cực 60 phút; CCU hiện tại của top game.
4. **Kibana D4:** bảng cảnh báo; bảng trending.
5. **Lưu trữ và import:** export `dashboards/kibana/*.ndjson`. Script `dashboards/import_kibana.sh` gọi Saved Objects API.
6. **Grafana D5:** `dashboards/grafana/d5_system.json` gồm consumer lag, thông lượng, CPU/RAM từng pod, HDFS live nodes, dung lượng, block thiếu.

## Giai đoạn 5 — Kubernetes đầy đủ (§5.4)

1. **`k8s/hive/`:** PostgreSQL StatefulSet và Hive Metastore Deployment/Service (0,5 CPU, 1 GB).
2. **`k8s/monitoring/`:**
   - Values cho `kube-prometheus-stack`.
   - Bật Strimzi `kafkaExporter` và metrics JMX.
   - JMX exporter cho HDFS.
   - ServiceMonitor cho ES.
3. **`k8s/spark/`:**
   - ScheduledSparkApplication hằng ngày cho B1–B6.
   - SparkApplication cho S1–S3 (`restartPolicy: Always`).
   - Bật dynamic allocation với min 2, max 6 executor, executor 1 CPU / 2 GB.
4. **`k8s/crawlers/`:**
   - Thêm CronJob cho appdetails và SteamSpy (hằng ngày), AppList (hằng tuần).
   - Chạy 2–4 worker theo hash(appid) bằng Indexed Job hoặc `CRAWLER_WORKER_ID`.
5. **`scripts/deploy_all.sh`:**
   - Bỏ `|| true`.
   - Thêm các bước operator (Strimzi, ECK, Spark Operator), Hive, monitoring, `setup_hdfs_dirs.sh`, `setup_indices.py`, Spark apps, import dashboard.
   - Gắn tag image theo phiên bản.
6. **`docs/deployment_guide.md`:** hướng dẫn dựng k3s 3–4 VM từ đầu. Ghi phương án dự phòng giảm heap theo §9.

**Xong khi:** từ cụm trống, chạy `setup_cluster.sh` rồi `deploy_all.sh` → D1–D5 có dữ liệu. Pod của cùng hệ thống nằm trên các node khác nhau.

## Giai đoạn 6 — Kiểm thử mở rộng và chịu lỗi (§8)

1. **SC1, SC2:** mở rộng script hiện có thành B2–B5 × {1, 2, 4, 6} executor và dữ liệu {1×, 2×, 5×, 10×}. Xuất CSV.
2. **SC3:** viết `tests/scalability/sc3_streaming_load_test.py`, replay với tốc độ tăng dần, partition 3→6, executor 2→6. Đo thông lượng, consumer lag, độ trễ end-to-end (`event_time` → thời điểm ghi ES).
3. **SC4:** viết `tests/scalability/sc4_es_query_load.py`, scale ES 2→3 node, chạy tải truy vấn, đo p50/p95 và thời gian phân bổ lại shard.
4. **FT1–FT6:**
   - Bổ sung `chaos_test_scenarios.sh` cho đủ 6 kịch bản.
   - FT3: kiểm tra không trùng/không mất bằng cách so số bản ghi trước và sau.
   - FT5: tắt hẳn một VM.
   - FT6: giả lập lỗi 429 bằng mock server hoặc biến môi trường trỏ endpoint lỗi.
5. Ghi kết quả vào `tests/results/*.csv`, chụp Grafana, quay video từng kịch bản.
6. Unit test pytest cho `text_preprocessor`, `aspect_dictionary`, luật review đáng ngờ, điều kiện cảnh báo S2/S3.

## Giai đoạn 7 — Bàn giao (§8.3, §10)

1. **Notebook:** `01_eda_raw_reviews`, `02_sentiment_aspect_exploration`, `03_scalability_fault_tolerance_analysis` (vẽ từ `tests/results`).
2. **`docs/architecture.md`:** kiến trúc, luồng dữ liệu, schema từng tầng, cơ chế hợp nhất Lambda.
3. Trả lời 5 câu hỏi §1.3 bằng số liệu thật.
4. Cập nhật Bảng 4 (dung lượng thực tế).
5. Cập nhật README mục 4.7.
6. Báo cáo cuối kỳ, slide, video demo.

## Thứ tự và ước lượng

| Giai đoạn | Phụ thuộc | Công (người-ngày) |
| --- | --- | --- |
| 1 Batch | — | 5 |
| 2 Speed + hợp nhất | 1 (baseline) | 4 |
| 3 Truy vấn / phân tích bổ sung | 1 | 2 |
| 4 Dashboard | 1, 2 | 3 |
| 5 K8s | có thể làm song song từ đầu | 5 |
| 6 Kiểm thử | 2, 5 | 4 |
| 7 Bàn giao | 4, 6 | 3 |

Tổng ~26 người-ngày, khoảng 2–3 tuần với nhóm 3–4 người. Theo rủi ro §9: dùng dữ liệu mẫu tĩnh để làm song song giai đoạn 1/2/4, và bắt đầu giai đoạn 5 ngay vì HDFS/Hive trên K8s là phần dễ bị chậm nhất.

Gợi ý phân công:

| Người | Phụ trách |
| --- | --- |
| A | Giai đoạn 1 và 3 |
| B | Giai đoạn 2 và 4 |
| C | Giai đoạn 5 |
| D | Crawler và giai đoạn 6 |

Cả nhóm cùng làm giai đoạn 7.
