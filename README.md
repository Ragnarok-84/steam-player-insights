# Steam Player Insights - Big Data Lambda Architecture

Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam theo **Kiến trúc Lambda (Lambda Architecture)**. Có hướng dẫn chạy thử local trên Linux/WSL2 và cấu hình triển khai phân tán trên **Kubernetes**.

Thành viên mới bắt đầu từ [mục 4](#4-chạy-local-cho-thành-viên-mới). Sơ đồ dưới đây mô tả kiến trúc mục tiêu; xem [giới hạn hiện tại](#47-giới-hạn-hiện-tại-cần-biết) trước khi chạy toàn bộ pipeline.

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
├── crawlers/                     # Thu thập dữ liệu từ Steam, SteamSpy & Kaggle Replay
│   ├── common/                   # Base client, rate limiter, cursor manager, kafka producer
│   ├── steam_reviews_crawler.py  # Thu thập review theo cursor
│   ├── steam_app_details_crawler.py # Thu thập metadata game
│   ├── steam_ccu_crawler.py      # Thu thập số người chơi đồng thời (CCU)
│   ├── steamspy_crawler.py       # Thu thập ước lượng người sở hữu, tags
│   ├── kaggle_replay_producer.py # Replay dữ liệu lịch sử vào Kafka để backfill/load test
│   └── config.py                 # Cấu hình crawler & Steam API Key
│
├── processing/                   # Mã nguồn xử lý dữ liệu Spark (Batch & Streaming)
│   ├── common/                   # Hàm tiện ích chung: SparkSession, từ điển khía cạnh, text utils
│   │   ├── spark_utils.py
│   │   ├── aspect_dictionary.py  # Từ điển khía cạnh: bug, performance, story, server...
│   │   └── text_preprocessor.py
│   ├── batch/                    # Batch layer (Spark Jobs B1 -> B6)
│   │   ├── b1_ingest_raw.py      # Đọc Kafka -> HDFS raw JSON gzip
│   │   ├── b2_clean_raw.py       # Raw JSON -> Parquet Clean (khử trùng, lọc spam)
│   │   ├── b3_game_daily_stats.py # Thống kê ngày: % positive, CCU peak/avg
│   │   ├── b4_playtime_buckets.py # Phân tích giờ chơi vs đánh giá (<2h, 2-10h, ...)
│   │   ├── b5_aspect_sentiment.py # Phân tích cảm xúc & khía cạnh phàn nàn
│   │   └── b6_trending_games.py   # Xếp hạng game xu hướng & tác động update/sale
│   └── streaming/                # Speed layer (Spark Structured Streaming S1 -> S3)
│       ├── s1_review_metrics.py  # Cửa sổ trượt 60p tính số review & % positive
│       ├── s2_review_bombing_detector.py # Phát hiện đột biến review tiêu cực (+3σ)
│       └── s3_ccu_spike_detector.py      # Phát hiện đột biến tăng/giảm CCU (>50%)
│
├── storage/                      # Thiết kế lược đồ lưu trữ & script khởi tạo
│   ├── hdfs/                     # Cấu trúc thư mục HDFS & schema partitioning
│   │   └── setup_hdfs_dirs.sh
│   ├── hive/                     # DDL tạo bảng Hive Metastore cho lớp Curated
│   │   └── create_tables.sql
│   └── elasticsearch/            # Mappings & templates cho 5 Elasticsearch indices
│       ├── indices_mapping.json
│       └── setup_indices.py
│
├── k8s/                          # Kubernetes Manifests, Helm Values & Operators
│   ├── kafka/                    # Cấu hình Kafka Strimzi (Cluster 3 brokers + Topics)
│   ├── hdfs/                     # Helm values triển khai HDFS (1 NameNode + 3 DataNodes)
│   ├── elasticsearch/            # ECK Operator cấu hình ES 3 nodes + Kibana
│   ├── hive/                     # Hive Metastore + PostgreSQL backend
│   ├── spark/                    # RBAC và SparkApplication CRDs
│   ├── crawlers/                 # K8s CronJob & Deployment cho các crawler workers
│   └── monitoring/               # kube-prometheus-stack values & Grafana config
│
├── dashboards/                   # Cấu hình trực quan hóa
│   ├── kibana/                   # Kibana Saved Objects (D1, D2, D3, D4)
│   ├── grafana/                  # Grafana Dashboard JSON (D5: Cluster, Kafka Lag, HDFS)
│   └── README.md
│
├── notebooks/                    # Jupyter Notebooks phân tích khám phá (EDA)
│   ├── 01_eda_raw_reviews.ipynb
│   ├── 02_sentiment_aspect_exploration.ipynb
│   └── 03_scalability_fault_tolerance_analysis.ipynb
│
├── tests/                        # Kịch bản kiểm thử mở rộng & chịu lỗi
│   ├── scalability/              # Kịch bản SC1 -> SC4 (benchmark executor, scale data, streaming load)
│   │   ├── sc1_sc2_batch_benchmark.py
│   │   └── sc3_streaming_load_test.py
│   └── fault_tolerance/          # Kịch bản FT1 -> FT6 (chaos test: kill pod broker/datanode/spark)
│       └── chaos_test_scenarios.sh
│
├── containers/                   # Dockerfiles đóng gói container images cho Kubernetes
│   ├── Dockerfile.crawler
│   └── Dockerfile.spark
│
├── docs/                         # Tài liệu kỹ thuật & báo cáo
│   ├── architecture.md           # Chi tiết kiến trúc Lambda & luồng dữ liệu
│   └── deployment_guide.md       # Hướng dẫn dựng cụm K8s từ đầu
│
├── scripts/                      # Shell scripts tự động hóa vận hành
│   ├── setup_cluster.sh
│   ├── deploy_all.sh
│   └── teardown.sh
│
├── .env.example                  # Template biến môi trường
├── requirements.txt              # Thư viện Python phụ thuộc
└── README.md                     # Tài liệu tổng quan
```

---

## 3. Các phân vùng dữ liệu và Topics

### 3.1. Kafka Topics

* `steam.reviews.raw` (6 partitions, key: `appid`, retention: 7 ngày)
* `steam.players.ccu` (3 partitions, key: `appid`, retention: 7 ngày)
* `steam.games.meta` (3 partitions, key: `appid`, log compaction)
* `steam.alerts` (1 partition, key: `appid`, retention: 7 ngày)

### 3.2. HDFS Directory Layout

* Raw Layer theo B1 hiện tại: `/steam/raw/{topic_thay_dấu_chấm_bằng_gạch_dưới}/dt=YYYY-MM-DD/` (JSON Lines gzip), ví dụ `/steam/raw/steam_reviews_raw/`. Script khởi tạo HDFS tạo các thư mục `reviews`, `ccu`, `meta`; chúng không trùng tên đầu ra B1.
* Clean Layer:
  * `/steam/clean/reviews/dt=YYYY-MM-DD/` (Parquet Snappy)
  * `/steam/clean/ccu/dt=YYYY-MM-DD/` (Parquet)
  * `/steam/clean/games/` (Parquet)
* Curated Layer (Hive): `/steam/curated/{game_daily_stats|aspect_stats|playtime_buckets|trending}/`
* Streaming Checkpoints: `/steam/checkpoints/{job_name}/`

### 3.3. Elasticsearch Indices

* `batch_game_daily`: Thống kê ngày theo game
* `batch_aspect_stats`: Thống kê khía cạnh phàn nàn theo tháng
* `realtime_game_metrics`: Chỉ số thời gian thực (cửa sổ 5/60 phút)
* `reviews_search`: Chỉ mục toàn văn review tiếng Anh kèm điểm cảm xúc
* `alerts`: Cảnh báo review bombing và đột biến người chơi

---

## 4. Chạy local cho thành viên mới

Các lệnh dưới đây dùng **Bash trên Linux hoặc WSL2 Ubuntu**. Người dùng Windows chạy toàn bộ lệnh trong WSL, không trộn Python/Java/Hadoop của Windows với bản Linux. macOS cần tự điều chỉnh đường dẫn JDK và cách cài công cụ; các lệnh `apt` chỉ dành cho Ubuntu/Debian.

Lộ trình: **Python + Java → HDFS → Spark ghi/đọc HDFS → Kafka + crawler → batch/streaming**. Kubernetes chỉ cần ở mục 5. Chạy được bài thử Spark/HDFS chưa có nghĩa là toàn bộ pipeline đã chạy hoàn chỉnh; xem giới hạn ở mục 4.7.

### 4.1. Công cụ và môi trường Python

| Thành phần | Cấu hình dùng trong hướng dẫn | Khi cần |
| --- | --- | --- |
| Python | 3.11, trong Conda hoặc venv | Crawler, Spark jobs |
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
conda create -n steam-insights python=3.11 -y
conda activate steam-insights
```

**Cách B — venv**, nếu đã có executable Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Nếu `.venv` cũ dùng Python khác, tạo lại bằng Python 3.11 sau khi đã kiểm tra không có dữ liệu riêng bên trong. Khi chuyển sang Conda, `.venv` cũ không còn cần thiết; xóa nó không ảnh hưởng dữ liệu HDFS. Không copy môi trường Python giữa Windows và WSL hoặc giữa các máy.

Sau khi kích hoạt môi trường đã chọn:

```bash
python --version
command -v python
python -m pip install --upgrade pip
python -m pip install -r requirements.txt "pyspark==3.5.7" "elasticsearch>=8.12.1,<9"
python -m pip check
python -c "import pyspark; print(pyspark.__version__)"
```

`requirements.txt` hiện dùng giới hạn `>=`, chưa phải lockfile. Các ràng buộc trong lệnh trên giữ Spark ở 3.5.7 và Elasticsearch Python client ở dòng 8; các thư viện khác vẫn có thể khác phiên bản giữa các lần cài. PySpark cài qua pip có kèm bộ chạy Spark, không cần cài thêm Spark riêng cho hướng dẫn local. Tham khảo [cài đặt PySpark 3.5.7](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html).

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

### 4.4. Chạy thử B2–B6 bằng dữ liệu mẫu

Đây là bài thử batch với **dữ liệu giả**, không cần Kafka/Steam API và không kiểm chứng B1. Script tạo 16 review trong 8 ngày và CCU tương ứng ở thư mục demo riêng từ bước 4.3:

```bash
python - <<'PY'
import json
import os
from datetime import datetime, timedelta, timezone
from processing.common.spark_utils import get_spark_session

root = os.environ["STEAM_DEMO_ROOT"]
spark = get_spark_session("Create_Demo_Data")
try:
    reviews, ccu = [], []
    for day in range(8):
        dt = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=day)
        for positive in (True, False):
            record = {
                "recommendationid": f"demo-{day}-{positive}", "appid": 730,
                "language": "english", "voted_up": positive,
                "review": "Great story and gameplay" if positive else "Server lag and bugs ruin gameplay",
                "timestamp_created": int(dt.timestamp()),
                "timestamp_updated": int(dt.timestamp()), "votes_up": 1,
                "playtime_at_review": 120.0, "playtime_forever": 180.0,
            }
            reviews.append((json.dumps(record),))
        ccu.append((730, dt.date(), 100 + day * 10))
    spark.createDataFrame(reviews, ["value"]).write.text(root + "/raw")
    spark.createDataFrame(ccu, ["appid", "dt", "player_count"]).write.parquet(root + "/ccu")
finally:
    spark.stop()
PY

export STEAM_HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"
```

Chạy lần lượt, chỉ chuyển sang job tiếp theo khi job trước thành công:

```bash
make b2 B2_ARGS="--raw $STEAM_DEMO_ROOT/raw --clean $STEAM_DEMO_ROOT/clean"
make b3 B3_ARGS="--clean-reviews $STEAM_DEMO_ROOT/clean --clean-ccu ${STEAM_HDFS_URI%/}$STEAM_DEMO_ROOT/ccu --curated $STEAM_DEMO_ROOT/curated"
make b4 B4_ARGS="--clean-reviews $STEAM_DEMO_ROOT/clean --curated $STEAM_DEMO_ROOT/curated"
make b5 B5_ARGS="--clean-reviews $STEAM_DEMO_ROOT/clean --curated $STEAM_DEMO_ROOT/curated"
make b6 B6_ARGS="--curated $STEAM_DEMO_ROOT/curated"
```

B3 nhận URI HDFS đầy đủ cho CCU để đi vào nhánh đọc HDFS của code hiện tại. Kiểm tra kết quả:

```bash
python - <<'PY'
import os
from processing.common.spark_utils import get_spark_session
spark = get_spark_session("Read_Demo_Result")
try:
    root = os.environ["STEAM_DEMO_ROOT"]
    assert spark.read.parquet(root + "/clean").count() == 16
    daily = spark.read.parquet(root + "/curated/game_daily_stats")
    assert daily.count() == 8
    daily.orderBy("dt").show()
    spark.read.parquet(root + "/curated/trending").orderBy("dt").show()
finally:
    spark.stop()
PY
```

Mỗi ngày có 2 review, tỷ lệ tích cực 50%. Muốn chạy lại bài thử, đặt `STEAM_DEMO_ROOT` mới rồi tạo lại dữ liệu; B2 ghi append nên chạy lại trên cùng thư mục sẽ cộng thêm dữ liệu.

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

Crawler review hiện dùng danh sách app cố định trong code và tối đa 50 trang/app; `TARGET_APP_LIST_PATH` chưa được crawler này đọc. Có thể dừng bằng `Ctrl+C`. Kiểm tra topic đã nhận review:

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

**B1 hiện có lỗi ghi text, cần sửa như mục 4.7 trước khi chạy luồng đầy đủ.** Các lệnh vận hành sau khi đã sửa và đủ dữ liệu:

```bash
make help
make -n batch
make b1 B1_ARGS="--topic steam.reviews.raw --brokers localhost:9092"
make b2 B2_ARGS="--raw /steam/raw/steam_reviews_raw --clean /steam/clean/reviews"
# Sau khi đã chuẩn bị cả review và CCU clean, chạy B3…B6 hoặc:
make batch
```

`make batch` chạy tuần tự B1 → B6, dừng khi lệnh lỗi; không dựng dịch vụ hay tự chuẩn bị dữ liệu CCU. Truyền tham số qua `B1_ARGS`…`B6_ARGS`, chọn Python bằng `PYTHON=/duong/dan/python` nếu cần. Luôn chạy tại gốc repo để import `processing.common` được phân giải.

S1 có thể thử độc lập với B1, xuất kết quả ra **console**:

```bash
python -m processing.streaming.s1_review_metrics \
  --brokers localhost:9092 --checkpoint /steam/checkpoints/s1_metrics
```

S1 chạy liên tục; dừng bằng `Ctrl+C`. Với checkpoint mới, S1 đọc offset `latest`: bật S1 trước rồi chạy crawler ở terminal thứ hai (cũng kích hoạt môi trường và cấu hình broker). Khi dùng lại checkpoint, Spark tiếp tục theo offset đã lưu. Dữ liệu review cũ có thể bị watermark loại bỏ; dùng event mới để kiểm tra thời gian thực.

### 4.7. Giới hạn hiện tại cần biết

README mô tả cả kiến trúc mục tiêu và mã đang triển khai; repo chưa có quy trình tự động chạy trọn hệ thống từ máy trống.

- **B1:** đưa `key`, `json_value`, `timestamp` vào text writer. Cần chọn đúng một cột chuỗi chứa JSON cùng cột partition `dt` trước `.text()`. Chưa sửa thì `make batch` sẽ dừng ở B1.
- **B3/B6:** B3 kiểm tra đường dẫn bằng `os.path.exists()` hoặc tiền tố `hdfs://`. Đường dẫn HDFS không có scheme có thể bị bỏ qua, dẫn đến thiếu `avg_ccu` mà B6 cần. Bài demo dùng URI đầy đủ và CCU mẫu; luồng thật cần bước làm sạch CCU (B2 hiện chỉ xử lý review).
- **Chạy lại batch:** B1 đọc từ earliest rồi append; B2 cũng append. Chưa có cơ chế chống ghi trùng giữa các lần chạy. B3–B6 ghi overwrite vào đầu ra của job; tránh dùng chung thư mục demo giữa các thành viên.
- **B6:** `lag(..., 7)` so với 7 bản ghi trước, không bảo đảm đúng 7 ngày nếu dữ liệu thiếu ngày.
- **Elasticsearch:** B3/B5 có nhánh ghi ES nhưng cần connector JVM riêng, cấu hình node/auth tương thích. Cài Python client chưa đủ. Một số lỗi ghi ES bị bắt và chỉ in cảnh báo, nên job kết thúc không chứng minh index có dữ liệu.
- **S1:** hiện ghi console, chưa ghi Elasticsearch; không có dashboard realtime tự xuất hiện sau khi bật job.
- **Cấu hình:** `HDFS_NAMENODE_URL` trong `.env` chưa được `spark_utils.py` sử dụng. Spark dựa vào `HADOOP_CONF_DIR`/URI đầu vào. Không export toàn bộ `.env` mặc định khi chưa dựng Hive/Elasticsearch.

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
| Thiếu cột `avg_ccu` khi chạy B6 | Xem giới hạn B3/B6 ở mục 4.7. |

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

Phần này dành cho nhóm đã có cụm Kubernetes, `kubectl`, Helm và tài nguyên lưu trữ. Hadoop local ở mục 4 không tự trở thành HDFS trong Kubernetes. Tài liệu [deployment_guide.md](docs/deployment_guide.md) mô tả cấu hình nhiều worker và tài nguyên dự kiến.

Các tài nguyên liên quan:

- `k8s/kafka/`: Strimzi và topics.
- `k8s/hdfs/`: HDFS nhiều DataNode.
- `k8s/elasticsearch/`: Elasticsearch và Kibana qua ECK.
- `k8s/hive/`: Hive Metastore/PostgreSQL.
- `k8s/spark/`, `k8s/crawlers/`: job và quyền truy cập.
- `k8s/monitoring/`: Prometheus/Grafana.

Trước khi dùng `scripts/setup_cluster.sh` và `scripts/deploy_all.sh`, rà soát chart/operator/API version, namespace, storage class, image registry và địa chỉ dịch vụ theo cụm thực tế. Script hiện không bảo đảm mọi thành phần đã sẵn sàng và chưa triển khai toàn bộ Hive/monitoring/Spark jobs. Image build local cần được push vào registry hoặc load vào các node để pod sử dụng được.

```bash
docker build -t steam-crawler:latest -f containers/Dockerfile.crawler .
docker build -t steam-spark:latest -f containers/Dockerfile.spark .
```

Dockerfile Spark đang dùng base Spark 3.5.0 trong khi hướng dẫn local dùng 3.5.7; cần đồng bộ base image, PySpark và connector trước khi triển khai. `requirements.txt` chưa khóa phiên bản nên build image hiện có thể kéo PySpark khác base image.

Khi Elasticsearch đã hoạt động và `.env` đã có endpoint/thông tin đăng nhập phù hợp:

```bash
python storage/elasticsearch/setup_indices.py
```

Kiểm tra index thực tế trên Elasticsearch sau khi chạy. Xem [dashboards/README.md](dashboards/README.md) và [notebooks/README.md](notebooks/README.md) cho phần trực quan hóa/khám phá; không giả định cấu hình dashboard và notebook trong sơ đồ đều đã có sẵn.

## 6. Kiểm thử mở rộng và chịu lỗi

Chỉ chạy trên cụm thử nghiệm đã chuẩn bị, sau khi pipeline cơ bản hoạt động:

```bash
python tests/scalability/sc1_sc2_batch_benchmark.py
python tests/scalability/sc3_streaming_load_test.py
```

Script `tests/fault_tolerance/chaos_test_scenarios.sh` chủ động tác động pod/dịch vụ để thử chịu lỗi. Đọc kịch bản và kiểm tra Kubernetes context/namespace trước khi chạy; đây không phải bước kiểm tra cài đặt local thông thường.
