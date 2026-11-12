# Chạy local trên Linux/WSL2 với Conda hoặc venv


Các lệnh dưới đây dùng **Bash trên Linux hoặc WSL2 Ubuntu**. Người dùng Windows chạy toàn bộ lệnh trong WSL, không trộn Python/Java/Hadoop của Windows với bản Linux. macOS cần tự điều chỉnh đường dẫn JDK và cách cài công cụ; các lệnh `apt` chỉ dành cho Ubuntu/Debian.

Lộ trình: **Python + Java → HDFS → Spark ghi/đọc HDFS → Kafka + crawler → batch/streaming**. Triển khai Kubernetes được hướng dẫn ở mục 5 của [README](../README.md). Chạy được bài thử Spark/HDFS chưa có nghĩa là toàn bộ pipeline đã chạy hoàn chỉnh; xem giới hạn hiện tại ở mục 7 của README.

**Demo nhẹ trên máy cá nhân:** dùng [hướng dẫn Docker Compose một replica](../infra/demo/README.md) để bật riêng HDFS (`batch`), Kafka (`streaming`) hoặc Elasticsearch/Kibana (`dashboard`). Cấu hình nằm trong `compose.demo.yaml`; mỗi container giới hạn 1 GB RAM. Spark chạy local trong Conda/venv, không cần dựng Kubernetes. Nếu chỉ xử lý dữ liệu từ Drive thành Parquet local, không cần bật các dịch vụ này.

### Nhận dữ liệu đã crawl từ Google Drive

Nhóm có thể phân công một thành viên crawl, nén dữ liệu rồi gửi lên Google Drive để các thành viên còn lại tải về xử lý. Người nhận dữ liệu **không cần crawl lại hoặc chạy Kafka/B1 để nhập lại bộ file này**; có thể chạy B2 trực tiếp từ raw sau khi chuẩn bị Python, Java và Spark.

Lệnh đóng gói, giải nén và kiểm tra số bản ghi nằm ở README mục 4.3 (A7 và cách B). Người gửi dừng vòng thu trước khi đóng gói. Gói `steam_data_YYYYMMDD.tar.gz` chứa sẵn đường dẫn `data/...`, nên giải nén **tại gốc repo**. Cấu trúc sau khi giải nén:

```text
data/
├── target_apps.json
├── target_apps.json.manifest.json
├── crawler_state/reviews_v2/    # chỉ cần nếu người nhận crawl tiếp
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

Lệnh upload trên dành cho lần nhập đầu tiên, khi ba thư mục đích chưa tồn tại trên HDFS. Với HDFS dùng chung, sử dụng đường dẫn riêng được quản trị viên cấp và thay cả đường dẫn upload lẫn tham số của B2. Bộ raw nhận từ Drive không cần state crawler để phân tích. Gói đã kèm `crawler_state/reviews_v2`, nên người nhận có thể crawl tiếp mà không trùng ([hướng dẫn thu thập](collection_guide.md)), với điều kiện từ đó họ là nơi crawl duy nhất của nhóm.

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

Nếu dùng Docker Desktop trên Windows, bật tích hợp với distro WSL và kiểm tra `docker info` chạy được trong WSL.

**Cách khuyên dùng:** `bash scripts/demo.sh up streaming` (Kafka có volume, tạo sẵn 4 topic), rồi làm theo README mục 4.11. Ví dụ `docker run` dưới đây là cách thử nhanh không lưu dữ liệu, cho client chạy trên cùng máy; client ở container/máy khác cần cấu hình advertised listeners phù hợp.

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
# CRAWLER_OUTPUT=kafka: .env mẫu đặt jsonl, nếu không đổi crawler sẽ ghi file thay vì Kafka.
# State riêng: review đã gửi Kafka không làm lệch tiến độ crawl vào data/raw.
CRAWLER_OUTPUT=kafka CURSOR_STATE_DIR=data/crawler_state/stream CRAWLER_REVIEW_DAYS=1 CRAWLER_REVIEW_BATCHES=2 \
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
make b1 PROFILE=cluster B1_ARGS="--topic all --brokers localhost:9092"
make b2 PROFILE=cluster B2_ARGS="--raw /steam/raw --clean /steam/clean"
make batch PROFILE=cluster  # B2 → B5 → B3 → B4 → B6 → B7 → B8 (B1 chạy riêng ở trên)
```

B1 lưu offset ở `/steam/checkpoints/b1_offsets/`, nên chạy lại chỉ đọc message mới. `make batch PROFILE=cluster` dừng khi lệnh lỗi; không dựng dịch vụ hay tự chuẩn bị dữ liệu. Truyền tham số cho từng job qua `ARGS` hoặc `B1_ARGS`…`B8_ARGS`, `S1_ARGS`…`S3_ARGS` (batch dùng `ARGS` của pipeline), chọn Python bằng `PYTHON=/duong/dan/python` nếu cần. Luôn chạy tại gốc repo để import `processing.common` được phân giải. Trên Windows hoặc khi không có `make`, dùng `python pipeline.py ...` (xem docstring trong `pipeline.py`).

Speed layer mặc định in ra **console**. Thêm `--es-nodes localhost:9200` hoặc đặt `WRITE_ES=1` để ghi `realtime_game_metrics`/`alerts`; S2/S3 còn ghi cảnh báo vào topic `steam.alerts` khi nguồn là Kafka:

```bash
make s1 PROFILE=cluster S1_ARGS="--brokers localhost:9092 --checkpoint /steam/checkpoints/s1_metrics"
make s2 PROFILE=cluster S2_ARGS="--brokers localhost:9092"
make s3 PROFILE=cluster S3_ARGS="--brokers localhost:9092"
```

Job streaming chạy liên tục; dừng bằng `Ctrl+C`. Với checkpoint mới, job đọc offset `latest` (đổi bằng `--starting-offsets earliest`): bật job trước rồi chạy crawler hoặc `python pipeline.py replay` ở terminal thứ hai. Dữ liệu cũ có thể bị watermark loại bỏ. Để kiểm tra thời gian thực, dùng crawler đẩy review/CCU mới vào Kafka (như trên) và `python pipeline.py inject` cho tình huống cảnh báo. `replay --retime` dồn review cũ vào thời điểm hiện tại, làm game nhỏ trông như bị bombing; chỉ dùng để thử tải (SC3). Cách bật/tắt cả ba job: `bash scripts/demo.sh stream-start|stream-stop`. S2/S3 cần baseline từ B7 (`curated/review_baseline`, `curated/ccu_baseline`); S2 dùng ngưỡng tuyệt đối dự phòng khi chưa có baseline.

Khi ghi ES, chạy `make merge PROFILE=cluster` (hoặc `python pipeline.py batch --es`) sau batch để xoá realtime view đã được batch view bao phủ và cập nhật `serving_state`.
