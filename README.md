# Steam Player Insights - Big Data Lambda Architecture

Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam theo **Kiến trúc Lambda (Lambda Architecture)**. Có hướng dẫn chạy thử local trên Linux/WSL2 và cấu hình triển khai phân tán trên **Kubernetes**.

Thành viên mới bắt đầu từ [mục 4](#4-chạy-local-cho-thành-viên-mới). Sơ đồ dưới đây mô tả kiến trúc mục tiêu; xem [giới hạn hiện tại](#47-giới-hạn-hiện-tại-cần-biết) trước khi chạy toàn bộ pipeline.

Để thu thập dữ liệu từ danh sách 1.000 game đã có, mở rộng lên 2.000 game và nhập review lịch sử vào file JSONL nén trên Windows, xem [hướng dẫn thu thập local](docs/collection_guide.md).

---

## 1. Tổng quan kiến trúc hệ thống

Dự án được thiết kế đáp ứng các yêu cầu của Bài tập lớn môn *Lưu trữ và Xử lý Dữ liệu lớn* (Hướng 1: Xây dựng và cài đặt kiến trúc dữ liệu lớn):

```
+---------------------------------------------------------------------------------------+
|                                    DATA SOURCES                                       |
|  Steam Store Reviews API | Steam Web API (CCU/AppList) | SteamSpy API | Kaggle Dataset |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
|                               INGESTION & BUFFER LAYER                                |
|   Python Crawlers (K8s CronJobs/Deployments)  &  Kaggle Replay Producer (Test/Backfill)  |
|                                           |                                           |
|                            Apache Kafka (Strimzi Operator)                            |
|     Topics: steam.reviews.raw | steam.players.ccu | steam.games.meta | steam.alerts   |
+------------------------------------+-------------------------------------+------------+
                                     |                                     |
                [Batch Pipeline]     |                                     | [Speed Pipeline]
                                     v                                     v
+-----------------------------------------------+ +-------------------------------------+
|                  BATCH LAYER                  | |             SPEED LAYER             |
|                                               | |                                     |
|  1. Ingestion: Kafka -> HDFS Raw (/steam/raw) | |  Spark Structured Streaming         |
|  2. Clean: Raw JSON -> Parquet (/steam/clean) | |  - Window 5m/60m, Watermark 10m     |
|  3. Batch Analytics (Spark Batch):           | |  - Realtime metrics: CCU, Reviews   |
|     - B3: Daily stats (Rating %, CCU peak)    | |  - Realtime Review Bombing Detector |
|     - B4: Playtime buckets analysis           | |  - CCU Spikes / Drop Detector       |
|     - B5: Aspect & Sentiment (VADER/Keywords) | |                                     |
|     - B6: Trending games ranking              | |                                     |
|                                               | |                                     |
|  Output: HDFS Curated (Hive Metastore)        | |  Output: Elasticsearch              |
|          & Elasticsearch (Batch Views)        | |          (Realtime Views & Alerts)  |
+-----------------------------------------------+ +-------------------------------------+
                                     \                                     /
                                      \                                   /
                                       v                                 v
+---------------------------------------------------------------------------------------+
|                               SERVING & PRESENTATION LAYER                            |
|                                                                                       |
|  - Elasticsearch (ECK): batch_game_daily, batch_aspect_stats, realtime_game_metrics,  |
|                         reviews_search, alerts                                        |
|  - Spark SQL + Hive Metastore (PostgreSQL): Ad-hoc Analytics trên HDFS Parquet        |
|  - Dashboards: Kibana (D1-D4: Overview, Game Detail, Realtime, Alerts)               |
|  - Monitoring: Prometheus + Grafana (D5: System Resources, Kafka Lag, HDFS health)    |
|  - Exploration: Jupyter Notebooks                                                     |
+---------------------------------------------------------------------------------------+
```

---

## 2. Cấu trúc thư mục dự án

```
steam-player-insights/
├── pipeline.py                   # Chạy batch/streaming không cần make (profile local | cluster)
├── crawlers/                     # Thu thập dữ liệu từ Steam, SteamSpy & dữ liệu lịch sử
│   ├── common/                   # Rate limiter, cursor, Kafka/JSONL producer, chia worker, dataset công khai
│   ├── steam_reviews_crawler.py  # Review theo cursor
│   ├── steam_app_details_crawler.py # Metadata game
│   ├── steam_ccu_crawler.py      # Số người chơi đồng thời (CCU)
│   ├── steamspy_crawler.py       # Ước lượng người sở hữu, tags
│   ├── steam_applist_crawler.py  # Danh sách toàn bộ app (hằng tuần)
│   ├── collect_local.py          # Crawl ra JSONL gzip local (không cần Kafka)
│   ├── historical_backfill.py    # Backfill review lịch sử
│   ├── kaggle_replay_producer.py # Replay dataset Kaggle vào Kafka
│   ├── replay_jsonl_to_kafka.py  # Replay raw JSONL vào Kafka (backfill/load test)
│   ├── update_targets_from_trending.py # Vòng phản hồi: thêm game trending vào danh sách theo dõi
│   └── config.py
│
├── processing/                   # Spark (Batch & Streaming)
│   ├── common/                   # SparkSession/HDFS/Hive helper, ghi ES, từ điển khía cạnh, text utils
│   ├── batch/
│   │   ├── b1_ingest_raw.py      # Kafka -> raw JSON gzip, lưu offset
│   │   ├── b2_clean_raw.py       # raw -> clean reviews/ccu/games (upsert, cờ đáng ngờ, ngôn ngữ)
│   │   ├── b3_game_daily_stats.py # Ngày/tuần/tháng, thể loại/nhà phát triển/giá, CCU
│   │   ├── b4_playtime_buckets.py # Giờ chơi vs đánh giá, early_negative_ratio, value_score
│   │   ├── b5_aspect_sentiment.py # Khía cạnh, VADER, TF-IDF từ khoá
│   │   ├── b6_trending_games.py   # Trending score/rank, tác động giảm giá
│   │   ├── b7_baselines.py        # Baseline 7 ngày cho S2/S3
│   │   └── b8_game_clusters.py    # K-Means hồ sơ trải nghiệm game
│   ├── streaming/
│   │   ├── s1_review_metrics.py  # Cửa sổ 5/60 phút -> realtime_game_metrics
│   │   ├── s2_review_bombing_detector.py # mean + 3σ & % tích cực giảm > 20 điểm -> alerts
│   │   └── s3_ccu_spike_detector.py      # CCU lệch > ±50% cùng khung giờ -> alerts
│   └── serving/merge_views.py    # Hợp nhất Lambda: xoá realtime view đã được batch bao phủ
│
├── storage/
│   ├── hdfs/setup_hdfs_dirs.sh
│   ├── hive/create_tables.sql    # DDL cũ, chỉ tham khảo (job tự đăng ký bảng khi có Metastore)
│   └── elasticsearch/            # Mapping 15 index & script tạo index
│
├── k8s/
│   ├── demo/                     # k3s 3 máy, mỗi dịch vụ một instance (namespace steam-demo)
│   ├── fault-test/               # Kafka 3 broker, HDFS 2 DataNode, observer, benchmark (steam-ft)
│   ├── kafka/ hdfs/ elasticsearch/ spark/ crawlers/ # Manifest/values theo thiết kế nhiều node (chưa chạy thử)
│   └── (chưa có) hive/, monitoring/
│
├── infra/
│   ├── k3s/                      # Cài node k3s, image Spark, smoke, benchmark, fault observer
│   └── demo/                     # Docker Compose một replica để thử local
│
├── dashboards/README.md          # Thiết kế D1–D5 (chưa có saved objects)
├── notebooks/README.md           # Kế hoạch notebook (chưa có .ipynb)
├── tests/
│   ├── make_sample_data.py       # Sinh dữ liệu mẫu có review bombing / CCU spike
│   ├── infrastructure/           # Pytest cho observer và benchmark k3s
│   ├── scalability/sc1_sc2_batch_benchmark.py # Script cũ, cần cập nhật
│   └── fault_tolerance/chaos_test_scenarios.sh # Script cũ, xem infra/k3s/FAULT_TEST.md
├── containers/                   # Dockerfile crawler & Spark 3.5.7 (kèm Kafka connector)
├── docs/                         # Hướng dẫn thu thập & triển khai
├── scripts/                      # demo.sh, k3s-demo.sh, k3s-fault-test.sh, deploy_all.sh...
├── .env.example
├── requirements.txt              # Lockfile Python 3.11 cho môi trường dev
├── requirements-spark.txt        # Thư viện cho image apache/spark
└── README.md
```

---

## 3. Các phân vùng dữ liệu và Topics

### 3.1. Kafka Topics

* `steam.reviews.raw` (6 partitions, key: `appid`, retention: 7 ngày)
* `steam.players.ccu` (3 partitions, key: `appid`, retention: 7 ngày)
* `steam.games.meta` (3 partitions, key: `appid`, log compaction)
* `steam.alerts` (1 partition, key: `appid`, retention: 7 ngày)

### 3.2. HDFS Directory Layout

* Raw Layer: `/steam/raw/{steam_reviews_raw|steam_players_ccu|steam_games_meta}/dt=YYYY-MM-DD/` (JSON Lines gzip). B1 và `crawlers.collect_local` ghi cùng layout, nên dữ liệu crawl local dùng trực tiếp làm raw.
* Clean Layer (Parquet Snappy):
  * `/steam/clean/reviews/dt=YYYY-MM-DD/`
  * `/steam/clean/ccu/dt=YYYY-MM-DD/`
  * `/steam/clean/games/` (snapshot mới nhất) và `/steam/clean/games_history/` (mọi snapshot, dùng cho phân tích giảm giá)
* Curated Layer: `/steam/curated/{game_daily_stats|game_period_stats|segment_stats|language_stats|review_sentiment|aspect_stats|keywords|playtime_buckets|game_retention|trending|trending_top|event_impact|review_baseline|ccu_baseline|game_clusters|cluster_profiles}/`. Khi đặt `HIVE_METASTORE_URI`, job tự đăng ký bảng `steam_analytics.<tên>`.
* Streaming Checkpoints: `/steam/checkpoints/{job_name}/`; offset B1 ở `/steam/checkpoints/b1_offsets/`.

### 3.3. Elasticsearch Indices

Mapping nằm ở `storage/elasticsearch/indices_mapping.json`:

* Batch layer: `batch_game_daily`, `batch_game_period`, `batch_segment_stats`, `batch_language_stats` (B3); `batch_playtime_buckets`, `batch_game_retention` (B4); `batch_aspect_stats`, `reviews_search`, `game_keywords` (B5); `batch_trending`, `batch_event_impact` (B6); `batch_game_clusters` (B8)
* Speed layer: `realtime_game_metrics` (S1, S3), `alerts` (S2, S3)
* `serving_state`: mốc cutoff giữa batch view và realtime view (`merge_views`)

---

## 4. Chạy local cho thành viên mới

Các lệnh dưới đây dùng **Bash trên Linux hoặc WSL2 Ubuntu**. Người dùng Windows chạy toàn bộ lệnh trong WSL, không trộn Python/Java/Hadoop của Windows với bản Linux. macOS cần tự điều chỉnh đường dẫn JDK và cách cài công cụ; các lệnh `apt` chỉ dành cho Ubuntu/Debian.

Lộ trình: **Python + Java → HDFS → Spark ghi/đọc HDFS → Kafka + crawler → batch/streaming**. Kubernetes chỉ cần ở mục 5. Chạy được bài thử Spark/HDFS chưa có nghĩa là toàn bộ pipeline đã chạy hoàn chỉnh; xem giới hạn ở mục 4.7.

**Demo nhẹ trên máy cá nhân:** dùng [hướng dẫn Docker Compose một replica](infra/demo/README.md) để bật riêng HDFS (`batch`), Kafka (`streaming`) hoặc Elasticsearch/Kibana (`dashboard`). Cấu hình nằm trong `compose.demo.yaml`; mỗi container giới hạn 1 GB RAM. Spark chạy local trong Conda/venv, không cần dựng Kubernetes. Nếu chỉ xử lý dữ liệu từ Drive thành Parquet local, không cần bật các dịch vụ này.

### Nhận dữ liệu đã crawl từ Google Drive

Nhóm có thể phân công một thành viên crawl, nén dữ liệu rồi gửi lên Google Drive để các thành viên còn lại tải về xử lý. Người nhận dữ liệu **không cần crawl lại hoặc chạy Kafka/B1 để nhập lại bộ file này**; có thể chạy B2 trực tiếp từ raw sau khi chuẩn bị Python, Java và Spark.

Người gửi nên dừng crawler hoặc chờ lượt crawl hoàn tất trước khi đóng gói, gửi kèm `target_apps.json` và `target_apps.json.manifest.json` nếu có. Người nhận tải file nén từ Drive về máy, giải nén và sắp xếp tại gốc repo theo cấu trúc:

```text
data/
├── target_apps.json
├── target_apps.json.manifest.json
└── raw/
    ├── steam_games_meta/
    │   └── dt=YYYY-MM-DD/part-*.jsonl.gz
    ├── steam_players_ccu/
    │   └── dt=YYYY-MM-DD/part-*.jsonl.gz
    └── steam_reviews_raw/
        └── dt=YYYY-MM-DD/part-*.jsonl.gz
```

Giữ nguyên các thư mục ngày `dt=...` và file `.jsonl.gz`: chỉ giải nén gói chia sẻ bên ngoài, **không cần giải nén tiếp từng file `.gz`**. Tránh để lồng thêm thư mục như `data/raw/raw/` hoặc `data/raw/ten-goi-nen/`. Nếu đã giải nén ba thư mục nguồn trực tiếp vào `data/`, chạy tại gốc repo (khi các thư mục đích chưa tồn tại):

```bash
mkdir -p data/raw
mv data/steam_games_meta data/steam_players_ccu data/steam_reviews_raw data/raw/
```

Kiểm tra có file dữ liệu và kiểm tra tính toàn vẹn gzip sau khi tải/giải nén:

```bash
find data/raw -type f -name '*.jsonl.gz' | head
find data/raw -type f -name '*.jsonl.gz' -exec gzip -t {} +
```

Lệnh `gzip -t` không in gì và trả mã thành công nếu các file gzip hợp lệ; bước này không kiểm tra tính đầy đủ của bộ dữ liệu. Đối chiếu số file/dung lượng với người gửi nếu cần. Raw và file nén đã được `.gitignore`; chia sẻ dữ liệu qua Drive, không đưa vào Git.

**Cách 1 — xử lý file local:** sau khi cài môi trường ở mục 4.1, chạy B2 tại gốc repo. Dùng URI `file://` tuyệt đối để Spark đọc file local kể cả khi máy đã cấu hình HDFS:

```bash
export PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=4 pyspark-shell'
python -m processing.batch.b2_clean_raw \
  --raw "file://$PWD/data/raw" \
  --clean "file://$PWD/data/lake/clean"
```

`--raw` và `--clean` là thư mục gốc: B2 đọc cả ba thư mục nguồn, khử trùng và ghi `clean/reviews`, `clean/ccu`, `clean/games`. B2 tính lại toàn bộ từ raw rồi ghi đè, nên chạy lại không sinh trùng. Cách nhanh hơn là chạy cả batch layer bằng `python pipeline.py batch`: pipeline tự đặt raw = `data/raw`, clean/curated = `data/lake/`.

**Cách 2 — dùng pipeline HDFS:** sau khi hoàn tất mục 4.2–4.3, đưa ba thư mục raw lên HDFS rồi chạy B2:

```bash
hdfs dfs -mkdir -p /steam/raw
hdfs dfs -put data/raw/steam_games_meta data/raw/steam_players_ccu data/raw/steam_reviews_raw /steam/raw/
hdfs dfs -ls /steam/raw/steam_reviews_raw
python -m processing.batch.b2_clean_raw --raw /steam/raw --clean /steam/clean
```

Lệnh upload trên dành cho lần nhập đầu tiên, khi ba thư mục đích chưa tồn tại trên HDFS. Với HDFS dùng chung, sử dụng đường dẫn riêng được quản trị viên cấp và thay cả đường dẫn upload lẫn tham số của B2. Bộ raw nhận từ Drive không cần checkpoint crawler để phân tích; nếu tiếp tục crawl từ lượt cũ thì cần người gửi bàn giao thêm state/cache theo [hướng dẫn thu thập local](docs/collection_guide.md).

### 4.1. Công cụ và môi trường Python

| Thành phần | Cấu hình dùng trong hướng dẫn | Khi cần |
| --- | --- | --- |
| Python | 3.11; môi trường nguồn dùng 3.11.16, trong Conda hoặc venv | Crawler, Spark jobs |
| Java | JDK 17 cho Spark 3.5; kiểm tra yêu cầu JDK của bản Hadoop đã chọn | Spark/Hadoop |
| PySpark | 3.5.7 | Xử lý dữ liệu |
| Hadoop | Bản binary đầy đủ, HDFS một NameNode + một DataNode cho local | Lưu trữ |
| GNU Make | Có lệnh `make` | Các lệnh `make b1`…`make b6` |
| Docker | Docker Engine hoặc Docker Desktop tích hợp WSL | Kafka local theo hướng dẫn này |

Với Ubuntu/Debian chưa có Java/Make:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk make
```

Clone repo bằng URL nhóm cung cấp, vào thư mục gốc chứa `requirements.txt` và `Makefile`. Trên WSL nên đặt repo trong filesystem Linux, ví dụ `~/projects/steam-player-insights`.

**Chọn một trong hai cách tạo môi trường**, không kích hoạt cả hai cùng lúc:

**Cách A — Conda**, nếu đã cài Miniconda/Anaconda:

```bash
conda create -n steam-insights python=3.11.16 -y
conda activate steam-insights
```

**Cách B — venv**, nếu đã có executable Python 3.11:

venv dùng Python đã cài trên máy, không tự tải Python. Kiểm tra phiên bản trước khi tạo; dùng Python 3.11, ưu tiên 3.11.16 để khớp máy nguồn. Nếu máy chỉ có Python 3.12/3.13, cài thêm Python 3.11 rồi dùng executable đó. Không cần cài Conda để dùng cách này.

```bash
python3.11 --version
python3.11 -m venv .venv
source .venv/bin/activate
```

Nếu báo thiếu module `venv` trên Ubuntu/Debian, cài gói `python3.11-venv` từ cùng nguồn đã cài Python 3.11 rồi chạy lại lệnh tạo môi trường. Nếu `.venv` cũ dùng Python khác, tạo môi trường mới bằng Python 3.11 sau khi đã kiểm tra không có dữ liệu riêng bên trong. Không copy môi trường Python giữa Windows và WSL hoặc giữa các máy.

**Cài thư viện — dùng chung cho Conda và venv:** sau khi kích hoạt môi trường đã chọn, chạy tại gốc repo:

```bash
python --version
command -v python
python -c "import sys; assert sys.version_info[:2] == (3, 11), 'Can dung Python 3.11'; print('Python executable:', sys.executable)"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip check
python -c "import pyspark; print(pyspark.__version__)"
```

`requirements.txt` khóa cả thư viện trực tiếp và phụ thuộc gián tiếp, lấy từ môi trường Conda `steam-insights` trên Linux với Python 3.11.16 ngày 07/10/2026. File chứa các gói pip với phiên bản cụ thể, không chứa đường dẫn máy nguồn hoặc gói hệ thống Conda, nên dùng chung cho cả Conda và venv. File này chỉ chốt thư viện Python, không cài Java, Hadoop hay các dịch vụ; chưa xác nhận cài mới trên venv hoặc Windows/macOS. PySpark cài qua pip có kèm bộ chạy Spark, không cần cài thêm Spark riêng cho hướng dẫn local. Tham khảo [cài đặt PySpark 3.5.7](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html).

### 4.2. Chọn đúng bản Hadoop và cấu hình HDFS

Nếu chưa cài Hadoop, tải bản **binary** từ [Apache Hadoop](https://hadoop.apache.org/releases.html), kiểm tra checksum và giải nén vào filesystem Linux. Đối chiếu yêu cầu Java của bản đó. Nếu đã có dữ liệu HDFS, giữ bản Hadoop tương thích và không tự nâng cấp/format trong quá trình làm theo hướng dẫn này.

Đặt đường dẫn theo **máy của bạn**, không copy đường dẫn của thành viên khác:

```bash
# Thay hai đường dẫn mẫu bằng thư mục thực tế trên máy.
export JAVA_HOME="/duong/dan/toi/jdk-17"
export HADOOP_HOME="/duong/dan/toi/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
hash -r

ls "$HADOOP_HOME/bin/hdfs" "$HADOOP_HOME/share/hadoop/common"
"$JAVA_HOME/bin/java" -version
command -v hdfs
hadoop version
```

Ví dụ Hadoop có thể ở `~/hadoop`, `~/hadoop/hadoop` hoặc `/opt/hadoop-<version>`. `HADOOP_HOME` phải là thư mục có cả `bin`, `etc`, `libexec` và `share/hadoop/common`. Trong WSL, kết quả `command -v hdfs` cần trỏ đến bản Linux đã chọn.

Lưu các dòng `export` đã điền đúng vào `~/.bashrc` nếu dùng Bash. Trong `$HADOOP_CONF_DIR/hadoop-env.sh`, đặt `export JAVA_HOME=...` đến JDK thực tế để tiến trình Hadoop cũng nhận cấu hình này.

Trong `core-site.xml`, thêm hoặc sửa thuộc tính bên trong `<configuration>`:

```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
```

Trong `hdfs-site.xml`, cấu hình local một DataNode như sau. Thay `TEN_USER` bằng tên tài khoản Linux; XML không tự thay `$HOME` theo cú pháp shell:

```xml
<property>
    <name>dfs.replication</name>
    <value>1</value>
</property>
<property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/TEN_USER/hadoopdata/hdfs/namenode</value>
</property>
<property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/TEN_USER/hadoopdata/hdfs/datanode</value>
</property>
```

Sao lưu file trước khi sửa; giữ các cấu hình khác và không khai báo trùng thuộc tính. Nếu đã có HDFS, dùng đúng thư mục dữ liệu hiện hữu của cùng cụm thay vì đổi sang ví dụ trên. Hai thuộc tính đường dẫn HDFS không nên được khai báo lại trong `mapred-site.xml`.

```bash
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey dfs.namenode.name.dir
hdfs getconf -confKey dfs.datanode.data.dir
hdfs getconf -confKey dfs.replication
```

Kết quả mong đợi: URI `hdfs://...`, hai đường dẫn dữ liệu Linux và replication `1`. **Chỉ với một cụm hoàn toàn mới**, sau khi đã xác nhận các thư mục cấu hình chưa có dữ liệu, khởi tạo một lần:

```bash
# CHỈ dành cho NameNode mới chưa từng được khởi tạo.
hdfs namenode -format
```

Không format lại khi khởi động thường ngày hoặc để chữa lỗi kết nối. Với dữ liệu cũ, đối chiếu `clusterID` trong `namenode/current/VERSION` và `datanode/current/VERSION`; chúng cần thuộc cùng cụm.

Bật dịch vụ nếu `jps` chưa có NameNode/DataNode:

```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
```

Đợi vài giây rồi kiểm tra:

```bash
jps
hdfs dfsadmin -report
hdfs dfs -ls /
```

Cần có `NameNode`, `DataNode` và `Live datanodes (1)`. Thư mục `/` trống là bình thường với cụm mới. UI NameNode thường ở <http://localhost:9870> nếu không đổi cổng. Cách bật trực tiếp trên không cần SSH localhost hoặc YARN.

```bash
hdfs dfs -mkdir -p /steam/raw /steam/clean /steam/curated /steam/checkpoints
```

Script `storage/hdfs/setup_hdfs_dirs.sh` đặt replication thành **3**, phù hợp mục tiêu cụm nhiều DataNode; không dùng nguyên script đó cho cấu hình local một DataNode. Với HDFS dùng chung, nhờ quản trị viên cấp thư mục/quyền ghi riêng cho mỗi thành viên.

### 4.3. Kiểm tra Spark kết nối HDFS

Trong terminal đã kích hoạt môi trường Python và đặt `HADOOP_CONF_DIR`, tại gốc repo:

```bash
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=4 pyspark-shell'
unset HIVE_METASTORE_URI ELASTICSEARCH_HOSTS
```

Nếu từng đặt `SPARK_HOME` cho bản Spark khác, bỏ biến đó khi dùng PySpark từ pip. Bước thử này chỉ cần HDFS, không cần Hive, Kafka hoặc Elasticsearch. Dùng thư mục thử riêng mỗi lần:

```bash
export STEAM_DEMO_ROOT="/steam/demo/$(id -un)/$(date +%Y%m%d-%H%M%S)"

python - <<'PY'
import os
from processing.common.spark_utils import get_spark_session

spark = get_spark_session("Local_HDFS_Check")
try:
    fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS")
    print("Filesystem:", fs)
    assert fs.startswith("hdfs://"), f"Spark chưa dùng HDFS: {fs}"
    path = os.environ["STEAM_DEMO_ROOT"] + "/smoke_test"
    spark.range(5).write.mode("errorifexists").parquet(path)
    result = spark.read.parquet(path)
    result.show()
    assert result.count() == 5
finally:
    spark.stop()
PY

hdfs dfs -ls "$STEAM_DEMO_ROOT/smoke_test"
```

Thành công khi hiện bảng gồm các số `0`–`4` và file Parquet trên HDFS. Giữ nguyên terminal để dùng các biến ở bước tiếp theo.

### 4.4. Chạy thử batch layer bằng dữ liệu mẫu

Bài thử dùng **dữ liệu giả**, không cần Kafka/Steam API/HDFS và không kiểm chứng B1. `tests/make_sample_data.py` sinh 30 game × 21 ngày cùng format `collect_local`, có một game bị review bombing (appid 1001), một game tăng CCU đột biến (appid 1002) và một đợt giảm giá giữa kỳ:

```bash
python tests/make_sample_data.py --out data/sample_raw
python pipeline.py batch --raw data/sample_raw --lake data/sample_lake
```

`pipeline.py batch` chạy B2 → B5 → B3 → B4 → B6 → B7 → B8 ở local mode và dừng khi một job lỗi. Kiểm tra kết quả:

```bash
python - <<'PY'
from processing.common.spark_utils import get_spark_session
spark = get_spark_session("Read_Sample_Result", master="local[2]")
try:
    root = "data/sample_lake/curated"
    spark.read.parquet(root + "/game_daily_stats").orderBy("appid", "dt").show(5)
    spark.read.parquet(root + "/trending_top").orderBy("trending_rank").show(5)
    spark.read.parquet(root + "/review_baseline").show(5)
finally:
    spark.stop()
PY
```

Thử speed layer trên cùng dữ liệu mà không cần Kafka (đọc dần từng file raw, in kết quả ra console):

```bash
python pipeline.py s2 --raw data/sample_raw --lake data/sample_lake -- --source file
```

Chạy lại không cần đổi thư mục: các job ghi đè đầu ra. Xoá `data/sample_lake/checkpoints/` khi muốn speed layer đọc lại từ đầu.

### 4.5. Kafka local và crawler

Nếu dùng Docker Desktop trên Windows, bật tích hợp với distro WSL và kiểm tra `docker info` chạy được trong WSL. Ví dụ dưới dùng một broker, cho client chạy trực tiếp trên cùng máy; client ở container/máy khác cần cấu hình advertised listeners phù hợp.

```bash
docker run -d --name steam-kafka -p 127.0.0.1:9092:9092 apache/kafka:3.9.1
docker logs --tail 50 steam-kafka
```

Đây là cấu hình thử local theo [Apache Kafka Quick Start](https://kafka.apache.org/39/getting-started/quickstart/), chưa có volume lưu bền vững. Nếu container đã tồn tại, dùng `docker start steam-kafka`. Sau khi broker sẵn sàng, tạo topics:

```bash
for topic in steam.reviews.raw steam.players.ccu steam.games.meta steam.alerts; do
  docker exec steam-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done

docker exec steam-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Topics local dùng một partition/replica để thử; cấu hình cụm mục 3 và `k8s/kafka/` dành cho triển khai phân tán.

Tạo `.env` nếu chưa có, tránh ghi đè cấu hình cá nhân:

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

Chỉnh broker, API key nếu API sử dụng cần key, và các địa chỉ dịch vụ theo máy. Không commit khóa thật. Crawler tự nạp `.env`, còn Makefile/Spark jobs không tự nạp file này. Cho một crawler local:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CRAWLER_WORKER_ID=0
export CRAWLER_TOTAL_WORKERS=1
python -m crawlers.steam_reviews_crawler
```

Crawler review đọc danh sách game từ `TARGET_APP_LIST_PATH` (mặc định `data/target_apps.json`, giới hạn `CRAWLER_TARGET_LIMIT`) và chỉ lấy các game thuộc worker hiện tại (`appid % CRAWLER_TOTAL_WORKERS`). Có thể dừng bằng `Ctrl+C`. Kiểm tra topic đã nhận review:

```bash
docker exec steam-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic steam.reviews.raw \
  --from-beginning --max-messages 1 --timeout-ms 10000
```

### 4.6. Connector, batch và streaming với Kafka

`kafka-python` chỉ phục vụ Python crawler, không thay thế Kafka connector của Spark. Với PySpark 3.5.7 từ pip (Scala 2.12), đặt:

```bash
export PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=4 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell'
```

Lần đầu cần mạng để tải JAR từ Maven. Nếu đổi phiên bản Spark/Scala, đổi tọa độ connector tương ứng; xem [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html).

Các lệnh vận hành khi đã có Kafka và HDFS:

```bash
make help
make -n batch
make b1 B1_ARGS="--topic all --brokers localhost:9092"
make b2 B2_ARGS="--raw /steam/raw --clean /steam/clean"
make batch          # B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8
```

B1 lưu offset ở `/steam/checkpoints/b1_offsets/`, nên chạy lại chỉ đọc message mới. `make batch` dừng khi lệnh lỗi; không dựng dịch vụ hay tự chuẩn bị dữ liệu. Truyền tham số qua `B1_ARGS`…`B8_ARGS`, `S1_ARGS`…`S3_ARGS`, chọn Python bằng `PYTHON=/duong/dan/python` nếu cần. Luôn chạy tại gốc repo để import `processing.common` được phân giải. Trên Windows hoặc khi không có `make`, dùng `python pipeline.py ...` (xem docstring trong `pipeline.py`).

Speed layer mặc định in ra **console**. Thêm `--es-nodes localhost:9200` hoặc đặt `WRITE_ES=1` để ghi `realtime_game_metrics`/`alerts`; S2/S3 còn ghi cảnh báo vào topic `steam.alerts` khi nguồn là Kafka:

```bash
make s1 S1_ARGS="--brokers localhost:9092 --checkpoint /steam/checkpoints/s1_metrics"
make s2 S2_ARGS="--brokers localhost:9092"
make s3 S3_ARGS="--brokers localhost:9092"
```

Job streaming chạy liên tục; dừng bằng `Ctrl+C`. Với checkpoint mới, job đọc offset `latest` (đổi bằng `--starting-offsets earliest`): bật job trước rồi chạy crawler hoặc `python pipeline.py replay` ở terminal thứ hai. Dữ liệu cũ có thể bị watermark loại bỏ; dùng event mới (replay có tùy chọn đổi thời gian) để kiểm tra thời gian thực. S2/S3 cần baseline từ B7 (`curated/review_baseline`, `curated/ccu_baseline`); S2 dùng ngưỡng tuyệt đối dự phòng khi chưa có baseline.

Khi ghi ES, chạy `make merge` (hoặc `python pipeline.py batch --es`) sau batch để xoá realtime view đã được batch view bao phủ và cập nhật `serving_state`.

### 4.7. Giới hạn hiện tại cần biết

README mô tả cả kiến trúc mục tiêu và mã đang triển khai; repo chưa có quy trình tự động chạy trọn hệ thống từ máy trống.

- **Chưa kiểm chứng đầu-cuối:** code xử lý B1–B8, S1–S3 (bản merge 26/10) chưa được chạy trọn trên Kafka + HDFS + ES thật, chưa chạy trên k3s. Báo lỗi và kết quả cho nhóm khi chạy thử.
- **Dữ liệu ít ngày:** B6 (so sánh 7 ngày với 7 ngày trước) và B7 (baseline 7 ngày) cần ≥ 14 ngày dữ liệu để có kết quả ý nghĩa. Bộ raw một ngày chỉ đủ kiểm tra luồng chạy.
- **Elasticsearch:** ghi bằng Python client (`processing/common/es_writer.py`), cần gói `elasticsearch` trên driver và executor; tạo index trước bằng `python storage/elasticsearch/setup_indices.py`. Lỗi ghi làm job thất bại.
- **Hive:** chưa triển khai Metastore. Không đặt `HIVE_METASTORE_URI` khi chưa có Metastore; truy vấn ad-hoc bằng `spark.read.parquet(...)` + `createOrReplaceTempView`.
- **Dashboard:** chưa có Kibana saved objects/Grafana; cần tự tạo data view trên các index.
- **Cấu hình:** `spark_utils.py` tự nạp `.env`. `HDFS_NAMENODE_URL` được dùng làm `fs.defaultFS`; để trống khi chạy local với `file://`. `pipeline.py --profile local` tự bỏ qua giá trị HDFS/Hive trong `.env`.

### 4.8. Lỗi thường gặp

| Triệu chứng | Kiểm tra/cách xử lý |
| --- | --- |
| `Invalid HADOOP_COMMON_HOME` | Kiểm tra `HADOOP_HOME/share/hadoop/common`, thư mục giải nén lồng nhau và biến `HADOOP_COMMON_HOME` cũ. |
| Sửa XML nhưng `getconf` vẫn trả đường dẫn cũ | Kiểm tra `command -v hdfs`, `HADOOP_CONF_DIR`, lưu file; tìm thuộc tính trùng trong `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`. |
| Đường dẫn `file:///D:/...` trên WSL | Đổi sang đường dẫn Linux trong file cấu hình thực sự được dùng; không format dữ liệu để chữa lỗi này. |
| `Connection refused` tới NameNode | Xem `jps`, `fs.defaultFS`, cổng và log NameNode. |
| Có NameNode nhưng 0 Live datanodes | Xem log DataNode, quyền thư mục và `clusterID`; không xóa thư mục dữ liệu để thử. |
| Spark dùng `file:///` hoặc ghi vào `/steam` local | Export đúng `HADOOP_CONF_DIR` trước khi tạo SparkSession. |
| `Failed to find data source: kafka` | Nạp Kafka connector JVM đúng phiên bản Spark/Scala, không chỉ cài `kafka-python`. |
| `NoBrokersAvailable` | Kiểm tra broker, cổng, advertised listeners và địa chỉ nhìn từ phía client. |
| `ModuleNotFoundError: processing` | Chạy tại gốc repo bằng `python -m ...` hoặc Makefile. |
| `JAVA_GATEWAY_EXITED` | Kiểm tra Java phù hợp, `JAVA_HOME`, Python đang dùng và log Java trước thông báo lỗi. |

Lệnh chẩn đoán (không cần in `.env` chứa khóa):

```bash
command -v python hdfs java
python --version
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey dfs.namenode.name.dir
hdfs getconf -confKey dfs.datanode.data.dir
rg -n 'dfs.namenode.name.dir|dfs.datanode.data.dir|D:/' "$HADOOP_CONF_DIR"
tail -n 60 "$HADOOP_HOME"/logs/*namenode*.log
tail -n 60 "$HADOOP_HOME"/logs/*datanode*.log
```

Nếu chưa cài `rg`, dùng `grep -RnE` thay cho `rg -n`. `export` chỉ ảnh hưởng terminal hiện tại và tiến trình con; khi mở terminal mới cần nạp lại biến và kích hoạt môi trường.

### 4.9. Những lần chạy sau và dừng dịch vụ

1. Vào gốc repo, kích hoạt `conda activate steam-insights` **hoặc** `source .venv/bin/activate`.
2. Nạp cấu hình Java/Hadoop và đặt lại `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, `PYSPARK_SUBMIT_ARGS` như trên.
3. Kiểm tra `jps`; chỉ bật tiến trình HDFS còn thiếu. Chạy `docker start steam-kafka` nếu cần Kafka.
4. Chạy job cần dùng. Không cài lại thư viện hoặc format NameNode mỗi lần mở máy.

Dừng các job streaming bằng `Ctrl+C`, rồi dừng dịch vụ local khi không dùng:

```bash
docker stop steam-kafka
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

Chỉ dừng các dịch vụ do mình quản lý; không dùng lệnh này để dừng cụm chung của nhóm.

## 5. Triển khai Kubernetes và các dịch vụ mở rộng

**Nhóm dùng máy cá nhân:** bắt đầu từ [hướng dẫn k3s ba máy và demo một instance](infra/k3s/README.md). Bộ `k8s/demo/` dùng một server + hai worker qua Tailscale, chia dịch vụ giữa ba máy, có bài smoke cross-node và Job B2 đọc raw từ Drive qua HDFS. Đây là mốc tích hợp Kubernetes; hướng dẫn Compose ở mục 4 chỉ dành cho thử local.

Sau khi demo hoạt động, chuyển sang [kiểm thử chịu lỗi và mở rộng lên bốn/năm máy](infra/k3s/FAULT_TEST.md). Bộ `k8s/fault-test/` tạo namespace riêng với Kafka ba broker (RF=3, min ISR=2), HDFS hai DataNode (replication=2), observer kiểm tra dữ liệu và Spark benchmark phân tán 1/2/4 executor.

Phần này dành cho nhóm đã có cụm Kubernetes, `kubectl`, Helm và tài nguyên lưu trữ. Hadoop local ở mục 4 không tự trở thành HDFS trong Kubernetes. Tài liệu [deployment_guide.md](docs/deployment_guide.md) mô tả cấu hình nhiều worker và tài nguyên dự kiến.

Các tài nguyên liên quan:

- `k8s/demo/`, `k8s/fault-test/`: bộ manifest đã dùng cho k3s (xem hai hướng dẫn trên).
- `k8s/kafka/`: Strimzi và topics.
- `k8s/hdfs/`: HDFS nhiều DataNode.
- `k8s/elasticsearch/`: Elasticsearch và Kibana qua ECK.
- `k8s/spark/`, `k8s/crawlers/`: RBAC và CronJob crawler CCU/reviews.
- Chưa có: `k8s/hive/` (Hive Metastore/PostgreSQL), `k8s/monitoring/` (Prometheus/Grafana), SparkApplication cho B1–B8/S1–S3.

Trước khi dùng `scripts/setup_cluster.sh` và `scripts/deploy_all.sh`, rà soát chart/operator/API version, namespace, storage class, image registry và địa chỉ dịch vụ theo cụm thực tế. Script hiện không bảo đảm mọi thành phần đã sẵn sàng và chưa triển khai toàn bộ Hive/monitoring/Spark jobs. Image build local cần được push vào registry hoặc load vào các node để pod sử dụng được.

```bash
docker build -t steam-crawler:latest -f containers/Dockerfile.crawler .
docker build -t steam-spark:latest -f containers/Dockerfile.spark .
```

`containers/Dockerfile.spark` dùng base `apache/spark:3.5.7-python3`, kèm Kafka connector, cài `requirements-spark.txt`. Image cho k3s (`infra/k3s/Dockerfile.spark-demo`) cài `requirements.txt` đã khóa. Mỗi khi code `processing/` thay đổi phải build lại image và tăng tag, vì image chứa bản sao code tại thời điểm build.

Khi Elasticsearch đã hoạt động và `.env` đã có endpoint/thông tin đăng nhập phù hợp:

```bash
python storage/elasticsearch/setup_indices.py
```

Kiểm tra index thực tế trên Elasticsearch sau khi chạy. Xem [dashboards/README.md](dashboards/README.md) và [notebooks/README.md](notebooks/README.md) cho phần trực quan hóa/khám phá; không giả định cấu hình dashboard và notebook trong sơ đồ đều đã có sẵn.

## 6. Kiểm thử mở rộng và chịu lỗi

Chỉ chạy trên cụm thử nghiệm đã chuẩn bị, sau khi pipeline cơ bản hoạt động:

Dùng [infra/k3s/FAULT_TEST.md](infra/k3s/FAULT_TEST.md): observer kiểm tra mất dữ liệu khi xoá pod Kafka/DataNode hoặc mất worker, và benchmark Spark-on-Kubernetes 1/2/4 executor (`bash scripts/k3s-fault-test.sh benchmark ...`). Kiểm thử offline của observer/benchmark:

```bash
python -m pytest tests/infrastructure
```

Chưa có: benchmark SC1/SC2 trên job B2–B5 thật (`tests/scalability/sc1_sc2_batch_benchmark.py` là bản cũ, truyền tham số `--scale` mà B2 không có), SC3 streaming, SC4 Elasticsearch, kịch bản FT3/FT4/FT6. Script `tests/fault_tolerance/chaos_test_scenarios.sh` là bản cũ, trỏ namespace khác và in PASS mà không kiểm tra dữ liệu; không dùng để kết luận.
