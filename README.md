# Steam Player Insights - Big Data Lambda Architecture

Hệ thống phân tích trải nghiệm người chơi game trên Steam theo **kiến trúc Lambda**. Chạy được trên một máy (Docker, dữ liệu file) và triển khai phân tán trên **Kubernetes**.

- Chạy thử nhanh: [mục 4](#4-chạy-thử-trên-một-máy-docker).
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

## 4. Chạy thử trên một máy (Docker)

Spark không ghi file được trực tiếp trên Windows (thiếu `winutils`), vì vậy dùng image Spark của repo. Trên Linux/WSL có PySpark 3.5.7 và JDK 17 thì có thể bỏ phần `docker run ...` và chạy thẳng `python pipeline.py ...`.

Các lệnh dưới chạy trong PowerShell, tại gốc repo.

### 4.1. Build image

```powershell
docker build -t steam-spark:dev -f containers/Dockerfile.spark .
```

### 4.2. Batch

Sinh dữ liệu mẫu rồi chạy batch:

```powershell
docker run --rm -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 tests/make_sample_data.py --out data/sample_raw
docker run --rm -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 pipeline.py batch --raw data/sample_raw
```

**Dữ liệu thật:**
- Giải nén vào `data/raw`, sao cho bên trong có `steam_reviews_raw/`, `steam_players_ccu/`, `steam_games_meta/`.
- Chạy lệnh batch ở trên, bỏ `--raw`.
- Khoảng 1 triệu review: cấp ≥ 8 GB RAM cho Docker và thêm `-e SPARK_DRIVER_MEMORY=6g`.
- Chạy lại an toàn: kết quả được tính lại toàn bộ và ghi đè, không trùng.

Kết quả nằm ở `data/lake/curated/*` (Parquet). Có thể xem bằng các notebook trong `notebooks/`.

### 4.3. Streaming đọc file (không cần Kafka)

Mỗi lần chạy lại, xoá `data\lake\checkpoints` trước. Dừng bằng `Ctrl+C`.

```powershell
docker run --rm -it -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 pipeline.py s2 --raw data/sample_raw -- --source file --max-files 100 --trigger "5 seconds"
```

Với dữ liệu mẫu, S2 báo `REVIEW_BOMBING` cho appid 1001. S1 và S3 chạy giống vậy.

### 4.4. Kafka, Elasticsearch, Kibana local

Bật các dịch vụ:

```powershell
docker network create steamnet
docker run -d --name steam-kafka --network steamnet -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://steam-kafka:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@steam-kafka:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_NUM_PARTITIONS=3 apache/kafka:3.8.1
docker run -d --name steam-es --network steamnet -e discovery.type=single-node -e xpack.security.enabled=false -e ES_JAVA_OPTS="-Xms1g -Xmx1g" docker.elastic.co/elasticsearch/elasticsearch:8.15.3
docker run -d --name steam-kibana --network steamnet -p 5601:5601 -e ELASTICSEARCH_HOSTS=http://steam-es:9200 docker.elastic.co/kibana/kibana:8.15.3
```

Tạo index và dashboard, rồi chạy batch có ghi ES:

```powershell
docker run --rm --network steamnet -e ELASTICSEARCH_HOSTS=http://steam-es:9200 -e KIBANA_HOST=http://steam-kibana:5601 -v "${PWD}:/opt/spark/work-dir" steam-spark:dev sh -c "python3 pipeline.py es-setup && python3 pipeline.py kibana-setup && python3 pipeline.py --es batch --raw data/sample_raw"
```

Đẩy dữ liệu vào Kafka (thời gian được dời về hiện tại), rồi chạy S2 đọc từ Kafka:

```powershell
docker run --rm --network steamnet -e KAFKA_BOOTSTRAP_SERVERS=steam-kafka:9092 -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 pipeline.py replay -- --raw data/sample_raw --rate 2000 --retime
docker run --rm -it --network steamnet -e ELASTICSEARCH_HOSTS=http://steam-es:9200 -v "${PWD}:/opt/spark/work-dir" steam-spark:dev python3 pipeline.py --es s2 -- --source kafka --brokers steam-kafka:9092 --starting-offsets earliest
```

Mở Kibana tại http://localhost:5601 → Dashboards. Có 4 dashboard:
- D1 Tổng quan.
- D2 Chi tiết game: gõ `appid:<id>` vào ô tìm kiếm để lọc.
- D3 Thời gian thực.
- D4 Cảnh báo & xu hướng.

### 4.5. Lệnh của `pipeline.py`

| Lệnh | Việc |
| --- | --- |
| `batch` | B2 → B5 → B3 → B4 → B6 → B7 → B8. Thêm `--es` để ghi ES và hợp nhất view; `--skip b8` để bỏ một job |
| `b1` … `b8`, `s1` `s2` `s3` | Chạy từng job; tham số riêng đặt sau `--` |
| `es-setup`, `kibana-setup`, `merge` | Tạo index; tạo dashboard; hợp nhất batch/realtime view |
| `replay`, `crawl`, `applist`, `feedback` | Đẩy JSONL vào Kafka; crawl local; lấy danh sách app (cần `STEAM_API_KEY`); thêm game trending vào danh sách theo dõi |

Tuỳ chọn chung: `--profile local|cluster`, `--raw`, `--lake`.
- `local`: đọc raw từ `data/raw`, ghi kết quả vào `data/lake`.
- `cluster`: lấy đường dẫn HDFS/ES/Hive từ biến môi trường (xem `.env.example`).

### 4.6. Crawl dữ liệu với API key

Tạo `.env` từ `.env.example`, điền `STEAM_API_KEY=...` (không commit `.env`), rồi làm theo [docs/collection_guide.md](docs/collection_guide.md).

---

## 5. Triển khai Kubernetes

Cài operator và monitoring (chỉ làm 1 lần):

```bash
./scripts/setup_cluster.sh
```

Dựng hạ tầng, pipeline và dashboard:

```bash
REGISTRY=<registry> ./scripts/deploy_all.sh
```

Tạo secret chứa API key Steam:

```bash
kubectl create secret generic steam-api --from-literal=STEAM_API_KEY=<key>
```

Chi tiết, cách nạp dữ liệu đã thu thập vào cụm, và cách truy cập Kibana/Grafana/Spark SQL: [docs/deployment_guide.md](docs/deployment_guide.md).

---

## 6. Kiểm thử

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
