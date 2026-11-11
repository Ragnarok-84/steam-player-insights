# Steam Player Insights - Big Data Lambda Architecture

Hệ thống phân tích trải nghiệm người chơi game trên Steam theo **kiến trúc Lambda**. Chạy được trên một máy (Docker, dữ liệu file) và triển khai phân tán trên **Kubernetes**.

- Nhận dữ liệu từ Drive, chạy batch, streaming và dashboard: [quy trình tại mục 4](#4-quy-trình-xử-lý-và-chạy-code).
- Linux/WSL2, Conda/venv và HDFS local: [hướng dẫn chi tiết](docs/local_setup_guide.md).
- Quyết định của nhóm: [docs/decisions.md](docs/decisions.md).
- Thu thập dữ liệu: [docs/collection_guide.md](docs/collection_guide.md).
- Kiến trúc chi tiết: [docs/architecture.md](docs/architecture.md).
- Triển khai cụm: [docs/deployment_guide.md](docs/deployment_guide.md).

---

## 1. Tổng quan kiến trúc

```
+---------------------------------------------------------------------------------------+
|                                    DATA SOURCES                                       |
|  Steam Store Reviews API | Steam Web API (CCU/AppList) | SteamSpy API | Kaggle Dataset |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
|                               INGESTION & BUFFER LAYER                                |
|   Python Crawlers (K8s CronJobs, chia worker)  &  Replay JSONL (backfill / load test)  |
|                            Apache Kafka (Strimzi, KRaft, 3 node)                      |
|     Topics: steam.reviews.raw | steam.players.ccu | steam.games.meta | steam.alerts   |
+------------------------------------+-------------------------------------+------------+
                [Batch Pipeline]     |                                     | [Speed Pipeline]
                                     v                                     v
+-----------------------------------------------+ +-------------------------------------+
|                  BATCH LAYER (hằng ngày)      | |             SPEED LAYER             |
|  B1: Kafka -> HDFS Raw (/steam/raw)           | |  Spark Structured Streaming         |
|  B2: Raw JSON -> Parquet (/steam/clean)       | |  S1: cửa sổ 5m & 60m/5m, wm 10m     |
|  B5: Cảm xúc VADER, khía cạnh, TF-IDF         | |  S2: Review bombing (mean + 3σ)     |
|  B3: Thống kê ngày/tuần/tháng/thể loại        | |  S3: Đột biến CCU (±50%)            |
|  B4: Giờ chơi, bỏ game sớm, "đáng tiền"       | |                                     |
|  B6: Trending + tác động giảm giá             | |                                     |
|  B7: Baseline cho S2/S3, B8: K-Means          | |                                     |
|  Output: HDFS Curated (Hive) + ES batch views | |  Output: ES realtime views & alerts |
+-----------------------------------------------+ +-------------------------------------+
                                     \   merge_views: batch thay realtime  /
                                      v                                   v
+---------------------------------------------------------------------------------------+
|                               SERVING & PRESENTATION LAYER                            |
|  - Elasticsearch (ECK): 15 index batch_*, reviews_search, realtime, alerts            |
|  - Spark SQL + Hive Metastore (PostgreSQL): ad-hoc trên HDFS Parquet                  |
|  - Kibana D1-D4 · Grafana D5 (Prometheus) · Jupyter notebooks                         |
+---------------------------------------------------------------------------------------+
```

---

## 2. Cấu trúc thư mục

```
steam-player-insights/
├── pipeline.py                  # Điểm chạy chung mọi job (local/cluster): python pipeline.py <lệnh>
├── Makefile                     # Lối tắt gọi pipeline.py (Linux/WSL)
├── crawlers/                    # Steam reviews/appdetails/CCU/AppList, SteamSpy, backfill lịch sử,
│                                #   replay JSONL -> Kafka, vòng phản hồi trending
├── processing/
│   ├── common/                  # SparkSession, đọc raw, ghi Parquet/Hive, ghi Elasticsearch, từ điển khía cạnh
│   ├── batch/                   # B1 ingest, B2 clean, B3 thống kê, B4 giờ chơi, B5 cảm xúc/TF-IDF,
│   │                            #   B6 trending, B7 baseline, B8 K-Means
│   ├── streaming/               # S1 chỉ số review, S2 review bombing, S3 đột biến CCU
│   └── serving/merge_views.py   # Hợp nhất batch view / realtime view
├── storage/                     # Mapping 15 index ES, script thư mục HDFS, truy vấn Hive mẫu
├── dashboards/                  # Kibana D1–D4 (setup_kibana.py + NDJSON), Grafana D5 (JSON)
├── k8s/                         # Kafka (KRaft), HDFS, ES/Kibana, Hive, Spark jobs, crawler, monitoring
├── containers/                  # Dockerfile.spark, Dockerfile.crawler
├── scripts/                     # setup_cluster.sh, deploy_all.sh, teardown.sh
├── tests/                       # unit/, scalability/ (SC1–SC4), fault_tolerance/ (FT1–FT6), make_sample_data.py
├── notebooks/                   # 01 EDA, 02 cảm xúc & khía cạnh, 03 scalability & fault tolerance
└── docs/                        # architecture, deployment_guide, collection_guide
```

---

## 3. Dữ liệu

| Tầng | Vị trí (cụm · local) | Định dạng |
| --- | --- | --- |
| Kafka | `steam.reviews.raw` (6p), `steam.players.ccu` (3p), `steam.games.meta` (3p, compacted), `steam.alerts` (1p) | JSON |
| Raw | `/steam/raw/steam_{reviews_raw,players_ccu,games_meta}/dt=` · `data/raw/...` | JSON Lines gzip |
| Clean | `/steam/clean/{reviews,ccu,games,games_history}` · `data/lake/clean` | Parquet snappy |
| Curated | `/steam/curated/*` (bảng Hive `steam_analytics.*`) · `data/lake/curated` | Parquet |
| Serving | Elasticsearch, 15 index (xem `storage/elasticsearch/indices_mapping.json`) | |

Raw dùng chung layout với output của crawler local, nên dữ liệu đã thu thập (giải nén vào `data/raw`) chạy được ngay, không cần Kafka.

---

## 4. Quy trình xử lý và chạy code

**Luồng phù hợp với cách nhóm đang làm:** một người thu thập dữ liệu, nén gửi qua Google Drive → các thành viên tải về `data/raw` → chạy batch local → kiểm tra kết quả → làm notebook/dashboard → tích hợp lên cụm k3s/HDFS.

Các lệnh Bash bên dưới chạy tại **gốc repo**, trên Linux hoặc WSL2, trong cùng môi trường Python. Windows nên dùng terminal WSL2; phương án Docker/PowerShell nằm ở mục 4.9. Cấu hình local dùng để phát triển và kiểm tra logic; kiểm thử phân tán vẫn cần cụm nhiều node.

### 4.1. Chọn phần cần chạy

| Mục tiêu | Cần bật | Lệnh chính / hướng dẫn |
| --- | --- | --- |
| Xử lý raw đã nhận từ Drive | Python + Java + PySpark | `python pipeline.py --profile local batch` |
| Xem kết quả, làm báo cáo | Kết quả batch + Jupyter | Mục 4.5 |
| Thử streaming từ file | Python + Java + PySpark; chạy batch trước để có baseline | Mục 4.6 |
| Xem dashboard | Spark + Elasticsearch + Kibana | Mục 4.7 |
| Thử luồng Kafka | Kafka + Spark + Kafka connector | Mục 4.8 |
| Tích hợp phân tán | Cụm k3s, HDFS và các dịch vụ | Mục 5 |

**Nhận raw từ Drive thì chưa cần HDFS, Kafka hay B1.** HDFS là kho dùng chung khi chạy trên cụm; Kafka phục vụ luồng dữ liệu; B1 lấy dữ liệu từ Kafka xuống raw. Đã có file raw thì bắt đầu từ B2.

### 4.2. Chuẩn bị môi trường

Nếu đã có môi trường Conda của dự án:

```bash
conda activate steam-insights
```

Nếu chưa có, tạo môi trường Python 3.11 theo [hướng dẫn Conda/venv](docs/local_setup_guide.md#41-công-cụ-và-môi-trường-python), rồi cài thư viện tại gốc repo:

```bash
python -m pip install -r requirements.txt
python -m pip check
```

Kiểm tra trước khi xử lý:

```bash
python --version
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

Dùng Python 3.11, JDK 17, PySpark 3.5.7 theo môi trường của repo. Không cần cài Hadoop để xử lý file local. Nếu chưa có `.env`, sao chép mẫu; giữ nguyên file cá nhân đã có:

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

Đặt cấu hình local trong terminal đang dùng:

```bash
export SPARK_MASTER='local[2]'
export SPARK_DRIVER_MEMORY=2g
export SPARK_SHUFFLE_PARTITIONS=4
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export WRITE_ES=0
export HIVE_METASTORE_URI_LOCAL=''
```

`local[2]` dùng hai luồng xử lý trên máy hiện tại; chưa phải Spark phân tán. Đây là cấu hình khởi đầu; nếu dữ liệu thật làm hết bộ nhớ, tăng bộ nhớ driver phù hợp RAM còn trống (ví dụ `export SPARK_DRIVER_MEMORY=4g`) rồi chạy lại. Xem log lỗi trước khi điều chỉnh.

`pipeline.py --profile local` đặt đường dẫn raw/clean/curated thành URI file local và tắt kết nối HDFS. Các module xử lý có nạp `.env`, nhưng biến đã `export` được ưu tiên. Lệnh `batch` không tự bật dịch vụ. Nếu mở terminal khác, kích hoạt lại môi trường và đặt lại các biến cần dùng.

### 4.3. Nhận và kiểm tra dữ liệu từ Google Drive

Giải nén **gói ZIP bên ngoài** vào repo, giữ nguyên các file `.jsonl.gz` bên trong:

```text
data/
├── target_apps.json
└── raw/
    ├── steam_reviews_raw/
    │   └── dt=YYYY-MM-DD/part-*.jsonl.gz
    ├── steam_players_ccu/
    │   └── dt=YYYY-MM-DD/part-*.jsonl.gz
    └── steam_games_meta/
        └── dt=YYYY-MM-DD/part-*.jsonl.gz
```

Không để lồng thành `data/raw/raw/...`. Người gửi nên gửi kèm target list, manifest và số file/dung lượng để mọi người dùng cùng một bộ dữ liệu. Không cần checkpoint crawler nếu chỉ phân tích bộ raw đã nhận.

```bash
du -sh data/raw
find data/raw -type f -name '*.jsonl.gz' | head
find data/raw -type f -name '*.jsonl.gz' -exec gzip -t {} +
```

`gzip -t` không in gì khi file hợp lệ; bước này không xác nhận bộ dữ liệu đã được gửi đầy đủ. Spark đọc trực tiếp gzip, không cần giải nén tiếp. Thư mục `dt` của raw là ngày nhập; ngày review lấy từ `timestamp_created`.

### 4.4. Chạy batch: raw → clean → curated

Nên thử trên dữ liệu mẫu trước, dùng đầu ra riêng với dữ liệu thật:

```bash
python tests/make_sample_data.py --out data/sample_raw
python pipeline.py --profile local batch --raw data/sample_raw --lake data/sample_lake
```

Sau khi mẫu chạy thành công, xử lý bộ dữ liệu thật đã nhận:

```bash
python pipeline.py --profile local batch --raw data/raw --lake data/lake
```

Lệnh lần lượt gọi các module sau; một job lỗi thì pipeline dừng:

| Thứ tự | Module trong `processing/batch/` | Chức năng | Đầu ra tiêu biểu |
| --- | --- | --- | --- |
| B2 | `b2_clean_raw.py` | Làm sạch, khử trùng review/CCU, chuẩn hóa metadata, gắn cờ review đáng ngờ | `clean/reviews`, `ccu`, `games`, `games_history` |
| B5 | `b5_aspect_sentiment.py` | Cảm xúc VADER, khía cạnh, từ khóa TF-IDF | `curated/review_sentiment`, `aspect_stats`, `keywords` |
| B3 | `b3_game_daily_stats.py` | Thống kê game theo ngày/tuần/tháng và nhóm game | `curated/game_daily_stats`, `game_period_stats`, `segment_stats`, `language_stats` |
| B4 | `b4_playtime_buckets.py` | Nhóm giờ chơi, review tiêu cực sớm, chỉ số đáng tiền | `curated/playtime_buckets`, `game_retention` |
| B6 | `b6_trending_games.py` | Tăng trưởng, xếp hạng trending, tác động giảm giá | `curated/trending`, `trending_top`, `event_impact` |
| B7 | `b7_baselines.py` | Mức bình thường để S2/S3 so sánh | `curated/review_baseline`, `ccu_baseline` |
| B8 | `b8_game_clusters.py` | Phân cụm K-Means các game có hồ sơ tương tự | `curated/game_clusters`, `cluster_profiles` |

B5 chạy trước B3 vì B3 dùng điểm cảm xúc. **`batch` không chạy B1.** Có thể chỉ chạy bước làm sạch hoặc bỏ phân cụm:

```bash
python pipeline.py --profile local b2 --raw data/raw --lake data/lake
python pipeline.py --profile local batch --raw data/raw --lake data/lake --skip b8
```

Các job sau cần đầu ra của các job trước; không chạy từng job theo thứ tự tùy ý. B2–B8 tính lại từ nguồn và ghi đè/upsert đầu ra; dùng `--lake` khác nếu cần giữ kết quả một lần chạy để so sánh. Không chạy hai pipeline cùng ghi vào một thư mục lake.

### 4.5. Kiểm tra và đọc kết quả

Sau khi batch dữ liệu thật chạy thành công:

```text
data/lake/
├── clean/          # Dữ liệu đã chuẩn hóa, khử trùng (Parquet)
└── curated/        # Các bảng kết quả phân tích (Parquet)
```

Thư mục `checkpoints/` được dùng khi chạy B1 hoặc streaming. Chỉ có `data/sample_lake` chưa chứng minh bộ dữ liệu thật đã được xử lý.

Đọc thử một bảng tổng hợp nhỏ bằng pandas:

```bash
python - <<'PY'
import pandas as pd
stats = pd.read_parquet('data/lake/curated/game_daily_stats')
print('Số dòng thống kê game/ngày:', len(stats))
print(stats[['appid', 'dt', 'total_reviews', 'positive_ratio_percent']].head(10).to_string(index=False))
PY
```

Mỗi dòng ở đây là một game/ngày, không phải một review. Để khám phá dữ liệu và làm báo cáo:

```bash
export LAKE_DIR="$PWD/data/lake"
jupyter lab notebooks/
```

- `01_eda_raw_reviews.ipynb`: khối lượng, độ phủ game/ngôn ngữ, giờ chơi, review đáng ngờ.
- `02_sentiment_aspect_exploration.ipynb`: cảm xúc, khía cạnh, từ khóa, phân cụm.
- `03_scalability_fault_tolerance_analysis.ipynb`: đọc kết quả kiểm thử trong `tests/results/`; cần chạy kiểm thử trước.

Muốn xem kết quả mẫu thì đặt `LAKE_DIR="$PWD/data/sample_lake"` trước khi khởi động Jupyter. Notebook có thể nạp nhiều dữ liệu vào RAM; bắt đầu từ bảng tổng hợp nếu máy ít bộ nhớ.

Review lịch sử giúp tăng chiều thời gian nhưng không bổ sung lịch sử CCU. CCU chỉ có một vài snapshot/game thì chưa đủ để kết luận xu hướng hay bất thường nhiều ngày; cần thu tiếp theo lịch trong [hướng dẫn thu thập](docs/collection_guide.md).

### 4.6. Thử streaming từ file, chưa cần Kafka

Sau batch mẫu ở mục 4.4, thử S2 trên cùng raw và lake để dùng baseline từ B7:

```bash
python pipeline.py --profile local s2 --raw data/sample_raw --lake data/sample_lake -- --source file --max-files 100 --trigger '5 seconds'
```

- S1: thống kê số review và tỷ lệ tích cực theo cửa sổ thời gian.
- S2: cảnh báo nghi vấn review bombing.
- S3: cảnh báo CCU tăng/giảm bất thường.

Thay `s2` bằng `s1` hoặc `s3` để thử từng job, mỗi lần một job nếu máy ít RAM. Job chạy liên tục, dừng bằng `Ctrl+C`. Với `WRITE_ES=0`, không truyền `--es`, kết quả được in ra console; không có cảnh báo ở một micro-batch có thể chỉ là chưa đủ điều kiện phát hiện.

Checkpoint lưu tiến độ trong lake, nên chạy lại mặc định là tiếp tục. Muốn đọc lại từ đầu, dừng job và dùng một đường dẫn checkpoint mới, ví dụ thêm sau `--`: `--checkpoint "file://$PWD/data/sample_lake/checkpoints/s2_trial_02"`. Giữ nguyên lake để tiếp tục dùng baseline; không cần xóa toàn bộ checkpoints. Dữ liệu cũ có thể bị watermark loại bỏ khi thời gian sự kiện lệch nhau nhiều; nguồn file là cách thử logic, không phải luồng Steam đang phát sinh trực tiếp.

### 4.7. Bật Elasticsearch + Kibana để xem dashboard

Cần Docker Engine/Docker Desktop và Compose v2 đang hoạt động; Spark vẫn chạy trong môi trường Python ở host. Chỉ bật nhóm dashboard:

```bash
bash scripts/demo.sh up dashboard
```

Trong terminal Python đã cấu hình ở mục 4.2, đặt kết nối cho demo một máy không bật xác thực:

```bash
export ELASTICSEARCH_HOSTS='http://localhost:9200'
export ELASTICSEARCH_USER=''
export ELASTICSEARCH_PASSWORD=''
export KIBANA_HOST='http://localhost:5601'

bash scripts/demo.sh init-es
python pipeline.py kibana-setup
python pipeline.py --profile local batch --raw data/raw --lake data/lake --es
```

`init-es` tạo 15 index theo mapping demo (1 shard, 0 replica). `kibana-setup` tạo dashboard; `batch --es` chạy lại batch, ghi kết quả vào ES và hợp nhất view. Chỉ tạo index/dashboard thì chưa có dữ liệu. Có thể đổi raw/lake sang bộ mẫu để thử nhẹ trước. Lệnh `pipeline.py es-setup` dùng mapping chuẩn cho cụm; với Compose một ES node, dùng `init-es` như trên.

Mở <http://localhost:5601> → Dashboards:

| Dashboard | Nội dung |
| --- | --- |
| D1 | Tổng quan thị trường |
| D2 | Chi tiết game; lọc bằng `appid:<id>` |
| D3 | Thời gian thực; cần chạy S1/S3 có ghi ES |
| D4 | Cảnh báo và trending; cảnh báo cần S2/S3 có ghi ES |

Để job streaming ghi ES, thêm `--es` trước dấu `--`, ví dụ:

```bash
python pipeline.py --profile local s2 --raw data/sample_raw --lake data/sample_lake --es -- --source file
```

Nếu dashboard trống, kiểm tra job đã ghi ES chưa, filter game và khoảng thời gian trên Kibana có chứa ngày dữ liệu không. Dừng dịch vụ sau buổi làm việc, giữ volume:

```bash
bash scripts/demo.sh stop dashboard
```

Grafana D5 theo dõi sức khỏe hạ tầng, không được bật trong nhóm Compose này; xem mục 5 khi triển khai monitoring trên cụm.

### 4.8. Tích hợp Kafka và B1 (sau khi luồng file đã chạy ổn)

Kafka nhận dữ liệu từ crawler/replay để B1 và các job streaming đọc độc lập. Để thử local:

```bash
bash scripts/demo.sh up streaming
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export SPARK_PACKAGES='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7'
```

Kafka connector cần mạng để tải JAR lần đầu. Trong terminal này, khởi động S2 (dùng lake riêng cho checkpoint Kafka, đọc baseline đã tạo từ batch mẫu):

```bash
python pipeline.py --profile local s2 --lake data/kafka_demo_lake -- --source kafka --brokers localhost:9092 --starting-offsets earliest --curated "file://$PWD/data/sample_lake/curated"
```

Ở **terminal thứ hai**, kích hoạt cùng môi trường Python rồi replay một lượng nhỏ review mẫu:

```bash
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
python pipeline.py replay -- --raw data/sample_raw --topics steam_reviews_raw --limit 2000 --rate 200 --retime
```

`--retime` đổi timestamp trên bản ghi gửi Kafka để mô phỏng sự kiện mới, không sửa file raw. Đây là dữ liệu replay phục vụ thử nghiệm. Có thể thêm `--es` cho S2 sau khi thiết lập kết nối ES như mục 4.7.

Nếu muốn kiểm tra B1 lấy dữ liệu Kafka xuống file local, chạy trong terminal có cấu hình Spark/Kafka ở trên (dừng S2 trước nếu cần nhường RAM):

```bash
python pipeline.py --profile local b1 --raw data/kafka_demo_raw --lake data/kafka_demo_lake -- --topic all --brokers localhost:9092
```

B1 ghi vào raw riêng và lưu offset để lần sau đọc tiếp. Không ghi replay vào `data/raw` gốc vì timestamp replay đã thay đổi. B1 không tự chạy batch; dữ liệu nhập từ Kafka có thể xử lý tiếp bằng `batch --raw data/kafka_demo_raw --lake data/kafka_demo_lake` khi đã có đủ các nguồn cần thiết.

Dừng job bằng `Ctrl+C`, rồi dừng Kafka khi không dùng:

```bash
bash scripts/demo.sh stop streaming
```

### 4.9. Phương án Docker cho Windows/PowerShell

Nếu không dùng môi trường Python trong WSL, có thể chạy Spark trong image của repo. Các lệnh sau là **PowerShell**, không trộn cú pháp với Bash ở trên:

```powershell
docker build -t steam-spark:dev -f containers/Dockerfile.spark .
docker run --rm -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 tests/make_sample_data.py --out data/sample_raw
docker run --rm -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 pipeline.py --profile local batch --raw data/sample_raw --lake data/sample_lake
```

Đổi thành `--raw data/raw --lake data/lake` để xử lý dữ liệu thật. Cấp đủ RAM cho Docker và driver; với khoảng một triệu review, hướng dẫn trước dùng Docker ≥ 8 GB và `-e SPARK_DRIVER_MEMORY=6g` (đặt trước tên image). Nhu cầu thực tế cần đo khi chạy. Container kết thúc nhưng kết quả vẫn nằm trong repo nhờ volume mount. Đây là phương án chạy local; cấu hình container gọi dịch vụ cần địa chỉ mạng phù hợp, không dùng `localhost` của host từ trong container.

### 4.10. Tra lệnh và xử lý lỗi thường gặp

| Lệnh | Công việc |
| --- | --- |
| `batch` | B2 → B5 → B3 → B4 → B6 → B7 → B8; `--es` ghi ES và merge; `--skip b8` bỏ phân cụm |
| `b1` … `b8`, `s1`, `s2`, `s3` | Chạy từng job; tham số riêng đặt sau `--` |
| `es-setup`, `kibana-setup`, `merge` | Tạo index chuẩn, tạo dashboard, hợp nhất batch/realtime |
| `replay`, `crawl`, `applist`, `feedback` | Replay vào Kafka, crawl local, lấy danh sách app, cập nhật game theo dõi từ trending |

Tuỳ chọn chung: `--profile local|cluster`, `--raw`, `--lake`. Với profile `cluster`, đường dẫn kho lấy từ các biến `HDFS_*`; `--raw`/`--lake` của pipeline không thay thế cấu hình cụm. Muốn xem tham số một job, ví dụ: `python -m processing.batch.b2_clean_raw --help`. Makefile là lối tắt gọi pipeline, ví dụ `make batch`, `make s2 ARGS="--source file"`.

| Hiện tượng | Kiểm tra / cách xử lý |
| --- | --- |
| `No module named ...` | Đúng Conda/venv chưa; đã cài `requirements.txt` bằng Python của môi trường đó chưa |
| `JAVA_GATEWAY_EXITED` | Xem lỗi Java đầu tiên trong log, kiểm tra `java -version`, `JAVA_HOME` và biến Spark; đây là lỗi khởi tạo Spark, chưa xác định được lỗi xử lý dữ liệu |
| Không thấy raw / sai filesystem | Kiểm tra cấu trúc mục 4.3, chạy rõ `--profile local`, truyền thư mục gốc `--raw data/raw` |
| Job lỗi vì thiếu bảng đầu vào | Chạy đúng thứ tự batch; đọc log job đầu tiên thất bại |
| Spark bị hết bộ nhớ | Giảm tải dịch vụ đang bật, kiểm tra RAM và tăng driver memory trong khả năng máy |
| Lỗi kết nối ES khi chỉ muốn tạo Parquet | Đặt `export WRITE_ES=0`, không truyền `--es` |
| Streaming không đọc lại file/message | Job nhớ tiến độ trong checkpoint; dùng checkpoint mới cho lượt thử mới, Kafka `starting-offsets` chỉ áp dụng khi chưa có checkpoint |

Muốn thu thêm dữ liệu/API key: làm theo [docs/collection_guide.md](docs/collection_guide.md). Không cần API key để xử lý bộ raw đã nhận.

---

## 5. Triển khai Kubernetes

Demo k3s thu gọn: [infra/k3s/README.md](infra/k3s/README.md). Kiểm thử chịu lỗi và benchmark nhiều máy: [infra/k3s/FAULT_TEST.md](infra/k3s/FAULT_TEST.md). Các cấu hình này nằm trong `k8s/demo/` và `k8s/fault-test/`, dùng riêng với bộ triển khai đầy đủ dưới đây.

### 5.1. Khi nào đưa dữ liệu lên HDFS?

Khi làm local theo mục 4, mỗi thành viên đọc raw trên máy mình. Khi chạy trên cụm, upload bộ raw **một lần** lên HDFS để Spark trên các node cùng truy cập; không cần mỗi thành viên upload một bản. Luồng xử lý là `raw trên HDFS → Spark → clean/curated trên HDFS + kết quả ES`. HDFS lưu dữ liệu; Spark thực hiện tính toán.

Với bộ demo `steam-demo`, trước tiên dựng cụm, triển khai dịch vụ, build/import image và chạy smoke theo [hướng dẫn k3s](infra/k3s/README.md). Sau đó, trên VM server đã cấu hình `kubectl`, giải nén dữ liệu từ Drive vào `data/raw` trong repo rồi chạy:

```bash
bash scripts/k3s-demo.sh upload-raw
bash scripts/k3s-demo.sh b2
kubectl -n steam-demo exec namenode-0 -- hdfs dfs -ls /steam/raw /steam/clean
```

Upload từ chối nếu thư mục nguồn tương ứng đã tồn tại trên HDFS để tránh nhập trùng. Lệnh `b2` tạo Job và đợi hoàn thành; nếu Job cũ còn tồn tại, script yêu cầu kiểm tra log trước khi chủ động xóa riêng Job đó để chạy lại.

**Bộ demo hiện cung cấp smoke và B2, chưa tự chạy toàn bộ B1–B8/S1–S3.** B2 demo chạy Spark local trong pod, chưa chứng minh driver/executor phân tán. Hướng dẫn benchmark Spark phân tán nằm trong `infra/k3s/FAULT_TEST.md`; bộ triển khai đầy đủ có phạm vi riêng ở mục 5.2. Các cấu hình cụm hiện mới được kiểm tra tĩnh, chưa có kết quả chạy trên cụm thật.

### 5.2. Bộ triển khai đầy đủ

Chỉ dùng các lệnh dưới sau khi đã chuẩn bị cụm và cấu hình theo [deployment guide](docs/deployment_guide.md); không dùng chúng để bật Compose local hoặc thay cho script `steam-demo`.

Cài operator và monitoring (chỉ làm 1 lần):

```bash
./scripts/setup_cluster.sh
```

Dựng hạ tầng, pipeline và dashboard:

```bash
REGISTRY='YOUR_REGISTRY' ./scripts/deploy_all.sh
```

Tạo secret chứa API key Steam:

```bash
kubectl create secret generic steam-api --from-literal=STEAM_API_KEY='YOUR_STEAM_API_KEY'
```

Chi tiết, cách nạp dữ liệu đã thu thập vào cụm, và cách truy cập Kibana/Grafana/Spark SQL: [docs/deployment_guide.md](docs/deployment_guide.md).

---

## 6. Kiểm thử

Kiểm thử offline observer/benchmark k3s: `python -m pytest tests/infrastructure`.

| Loại | Lệnh | Ghi chú |
| --- | --- | --- |
| Unit | `python -m pytest tests/unit` (hoặc chạy trong image) | Luật S2/S3, B4, crawler, vòng phản hồi |
| SC1/SC2 batch | `python tests/scalability/sc1_sc2_batch_benchmark.py --mode local` (hoặc `k8s`) | Executor 1/2/4/6; dữ liệu 1/2/5/10x |
| SC3 streaming | `python tests/scalability/sc3_streaming_load_test.py --brokers <kafka> --label p6_e4` | Thông lượng, consumer lag, thời gian micro-batch |
| SC4 serving | `python tests/scalability/sc4_es_query_load.py --label es3 --wait-green` | p50/p95/p99 khi ES tăng từ 2 lên 3 node |
| FT1–FT5 | `./tests/fault_tolerance/chaos_test_scenarios.sh ft1` … `ft5 <node>` | **Xoá pod / drain node thật**; chỉ chạy trên cụm thử nghiệm |
| FT6 | `python tests/fault_tolerance/ft6_crawler_rate_limit.py` | Mock API trả 429; chạy local |

Kết quả CSV nằm ở `tests/results/`. Biểu đồ vẽ trong `notebooks/03_scalability_fault_tolerance_analysis.ipynb`. File CSV bị gitignore; muốn đưa vào báo cáo thì dùng `git add -f`.

---

## 7. Đã kiểm chứng và giới hạn

**Đã chạy thử** (Docker, dữ liệu mẫu):
- B1–B8 và hợp nhất view.
- S1–S3, với nguồn file và nguồn Kafka.
- Ghi đủ 15 index ES.
- Kibana D1–D4 render được; Grafana D5 import được.
- Hive Metastore 3.1.3 + PostgreSQL: bảng tự đăng ký, truy vấn Spark SQL được.
- Unit test, SC1–SC4 (bản rút gọn), FT6.
- Manifest K8s hợp lệ schema (kubeconform, kể cả CRD).

**Chưa chạy trên cụm K8s thật:** `deploy_all.sh`, FT1–FT5, và SC ở quy mô cụm. Lần triển khai đầu cần theo dõi log.

**Giới hạn:**
- Chỉ phân tích văn bản review tiếng Anh; ngôn ngữ khác chỉ dùng nhãn `voted_up`.
- TF-IDF tính theo game (mỗi game là một văn bản).
- Panel HDFS trong Grafana D5 dựa trên trạng thái pod, không dùng JMX của NameNode.
