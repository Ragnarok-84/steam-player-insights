# Kiến trúc Steam Player Insights

Hệ thống theo kiến trúc Lambda: dữ liệu đi qua Kafka, chia hai nhánh batch và speed, rồi hợp nhất ở serving layer (Elasticsearch + Hive).

## 1. Luồng dữ liệu

```
Reviews / AppDetails / Store / Histogram / CCU / AppList   Raw đã crawl (replay, thử tải)
            │ crawlers/* (CronJob, chia worker theo appid % N)      │ crawlers/replay_jsonl_to_kafka.py
            ▼                                                       ▼
   Kafka: steam.reviews.raw (6p) · steam.players.ccu (3p) · steam.games.meta (3p, compacted) · steam.alerts (1p)
            │                                                       │
   BATCH (hằng ngày)                                         SPEED (liên tục)
   B1 Kafka → /steam/raw/{steam_reviews_raw,...}/dt=        S1 cửa sổ 5m & 60m/5m → realtime_game_metrics
   B2 raw → /steam/clean/{reviews,ccu,games,games_history}  S2 60m/10m vs baseline 7 ngày → alerts + steam.alerts
   B5 cảm xúc, khía cạnh, TF-IDF                             S3 10m vs CCU cùng khung giờ → realtime + alerts
   B3 thống kê ngày/tuần/tháng/thể loại/ngôn ngữ
   B4 giờ chơi, bỏ game sớm, "đáng tiền"
   B6 trending, tác động giảm giá
   B7 baseline cho S2/S3      B8 K-Means
            │                                                       │
            ▼                                                       ▼
   /steam/curated/* (Parquet, bảng Hive steam_analytics)   Elasticsearch realtime_game_metrics, alerts
   Elasticsearch batch_* , reviews_search, game_keywords
            └──────────── processing/serving/merge_views.py ────────┘
               xoá realtime có window_end <= batch_cutoff, lưu serving_state
                                  ▼
            Kibana D1–D4 · Grafana D5 · Spark SQL ad-hoc · Jupyter
```

Vòng phản hồi: `crawlers/update_targets_from_trending.py` (`pipeline.py feedback`) thêm game **đang lên** trên bảng most-played của Steam (mới vào top hoặc tăng hạng so với tuần trước) vào `data/target_apps.json`, nhóm `feedback`. Tín hiệu lấy từ Steam vì B6 chỉ xếp hạng game đã theo dõi. Chỉ thêm, không xoá; lượt crawl sau tự lấy dữ liệu game mới. Ngưỡng cảnh báo đọc từ biến môi trường hoặc ConfigMap.

## 2. Thứ tự job batch và lý do

`B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8 → merge`

- B5 chạy trước B3, vì B3 lấy điểm cảm xúc trung bình từ `curated/review_sentiment`.
- B6 cần `game_daily_stats` của B3.
- B7 cần dữ liệu clean mới nhất để tính baseline, và S2/S3 đọc lại baseline ở mỗi micro-batch.
- Demo cảnh báo: `crawlers/demo_inject.py` (`pipeline.py inject`) bơm sự kiện `source=demo_injection` vào Kafka; S2/S3 xử lý như thường, B2 loại các bản ghi này khỏi batch.
- B2–B8 tính lại toàn bộ từ master dataset rồi ghi đè (dynamic overwrite theo `dt`). Chạy lại không sinh trùng.
- B1 lưu offset Kafka ở `/steam/checkpoints/b1_offsets/<topic>` và đọc tiếp từ đó. Nếu job lỗi giữa chừng, có thể ghi lặp một phần (at-least-once), nhưng B2 sẽ khử trùng.

## 3. Lược đồ theo tầng

| Tầng | Đường dẫn / index | Khoá | Ghi chú |
| --- | --- | --- | --- |
| Raw | `/steam/raw/steam_{reviews_raw,players_ccu,games_meta}/dt=` | — | JSON Lines gzip. Trùng layout với `crawlers.collect_local`, nên dữ liệu crawl local dùng thẳng được |
| Clean | `clean/reviews/dt=` | `recommendationid` | Giữ bản `timestamp_updated` mới nhất. Có `is_english`, `is_suspicious` và `suspicious_reason` |
| Clean | `clean/ccu/dt=` | `(appid, timestamp)` | Thêm `ts`, `hour` |
| Clean | `clean/games`, `clean/games_history` | `appid` / `(appid, timestamp)` | Gộp appdetails với snapshot store của Steam (tag + trọng số, tổng review, early access). Có `price_bucket` |
| Clean | `clean/review_daily_counts` | `(appid, dt)` | Số review up/down theo ngày từ histogram Steam, giữ snapshot mới nhất mỗi ngày |
| Curated | `game_daily_stats` | `(appid, dt)` | review, % tích cực, giờ chơi trung vị, cảm xúc TB, CCU TB/đỉnh, tên/thể loại/giá |
| Curated | `game_period_stats`, `segment_stats`, `language_stats` | — | rollup tuần/tháng; thể loại / nhà phát triển / khoảng giá |
| Curated | `aspect_stats`, `review_sentiment`, `keywords` | — | B5 |
| Curated | `playtime_buckets`, `game_retention` | — | B4: `early_negative_ratio`, `value_score` |
| Curated | `trending`, `trending_top`, `event_impact` | `(appid, dt)` | B6: cửa sổ 7 ngày theo lịch (`rangeBetween`), z-score |
| Curated | `review_baseline`, `ccu_baseline` | `appid` / `(appid, hour)` | B7. Review: 7 ngày đủ dữ liệu của histogram Steam; độ lệch chuẩn theo giờ = √(mean + Var(ngày)/24²); dưới 30 review thì % tích cực lấy theo tỷ lệ trọn đời của Steam. CCU: TB cùng khung giờ UTC |
| Curated | `game_clusters`, `cluster_profiles` | `appid` | B8 |
| Serving | ES `batch_*`, `reviews_search`, `game_keywords` | id ghép từ khoá | upsert, nên chạy lại không trùng |
| Serving | ES `realtime_game_metrics` | `metric_type_appid_window_size_window_start` | upsert theo cửa sổ (update mode) |
| Serving | ES `alerts` + Kafka `steam.alerts` | `alert_type_appid_window_start` | |

Mapping ES đầy đủ nằm ở `storage/elasticsearch/indices_mapping.json`.

## 4. Luật phát hiện bất thường

- **S2 review bombing** (cửa sổ 60 phút, trượt 10 phút, watermark 15 phút). Cảnh báo khi thoả cả ba:
  - `negative ≥ S2_MIN_NEGATIVE` (mặc định 10);
  - `negative > mean + 3σ` của số review tiêu cực mỗi giờ trong 7 ngày, có tính cả các giờ bằng 0;
  - `% tích cực < baseline − 20`.

  Game chưa có baseline dùng ngưỡng dự phòng: ≥ 50 review và ≤ 35% tích cực. Với game ít review, luật 3σ trên số đếm nhỏ thỉnh thoảng bắt cụm tiêu cực thật (ví dụ Overwatch ngày 12/11: 10 review tiêu cực/giờ so với 2,4 ± 1,6); chỉnh bằng `S2_SIGMA`, `S2_MIN_NEGATIVE`.
- **S3 CCU** (cửa sổ 10 phút). Cảnh báo `CCU_SPIKE` / `CCU_DROP` khi `|CCU − baseline| / baseline > 50%`, với baseline là CCU trung bình cùng giờ trong các ngày trước. Bỏ qua các game có baseline < 100. Chỉ khung giờ đã có CCU mới có baseline, nên cần vòng thu CCU chạy ≥ 24 giờ.

Unit test cho cả hai luật nằm ở `tests/unit/test_detection_rules.py`.

## 5. Chịu lỗi

| Thành phần | Cơ chế |
| --- | --- |
| Kafka | 3 node KRaft, RF=3, `min.insync.replicas=2`, producer `acks=all` |
| HDFS | 3 DataNode, `dfs.replication=3`, anti-affinity |
| Elasticsearch | 3 node, 1 replica, anti-affinity |
| Spark batch | Task chạy lại khi mất executor. Job idempotent (ghi đè/upsert) |
| Spark streaming | Checkpoint trên HDFS. Deployment `Recreate` tự tạo lại driver. Sink upsert theo id nên không trùng |
| Crawler | Rate limiter có backoff khi gặp 429, thử lại khi lỗi mạng. State review (`reviews_v2`) lưu trên PVC: crawl tăng dần, không ghi trùng kể cả khi bị ngắt. CronJob chạy lại theo lịch |

## 6. Quyết định thiết kế

- **Ghi ES bằng Python client** (`processing/common/es_writer.py`, bulk theo partition) thay cho connector JVM `elasticsearch-spark`: ít phụ thuộc phiên bản Scala/Spark hơn, và lỗi ghi được raise thay vì bị bỏ qua.
- **Hive Metastore 3.1.3** thay cho 4.x: Spark 3.5 dùng client Hive 2.3, không tương thích HMS 4 (lỗi `Invalid method name: 'get_table'`).
- **Spark trên K8s dùng client mode** (driver nằm trong pod CronJob/Deployment) thay cho SparkApplication: một image duy nhất cho mọi job, dùng chung `pipeline.py` ở local và trên cụm. Spark Operator vẫn được cài nếu nhóm muốn chuyển sang.
- **Tên thư mục raw** giữ theo topic (`steam_reviews_raw`...) thay cho `reviews/ccu/meta` như bảng §5.5b, để dữ liệu crawl local và output B1 có cùng layout.
- **Dashboard Kibana dùng Vega**, sinh từ code (`dashboards/setup_kibana.py`): định nghĩa nằm trong Git và import lại được trên cụm mới.
- **Đếm review theo ngày bằng histogram của Steam** (`appreviewhistogram`) cho B6/B7: review đã crawl ở game lớn chỉ là mẫu gần nhất, không dùng để đếm.
- **Thời gian ghi ES luôn là UTC có múi giờ** (`es_writer._jsonable`): PySpark trả `datetime` theo giờ máy, ES hiểu chuỗi không múi giờ là UTC.
- **Cutoff hợp nhất là thời điểm chính xác**, tối đa 00:00 UTC hôm nay: ES làm tròn `lte: "YYYY-MM-DD"` lên cuối ngày.
- **Key Kafka của `steam.games.meta` tách theo loại bản ghi** (`appid`, `appid:store`, `appid:histogram`), vì topic bị compact theo key.
