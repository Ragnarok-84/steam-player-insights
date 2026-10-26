# Demo trên máy cá nhân

Chạy tại gốc repo bằng Bash trên Linux/WSL2. Demo dùng Docker Compose với một instance cho mỗi dịch vụ và named volume để giữ dữ liệu khi dừng container. Spark chạy `local[2]` trong Conda hoặc venv của bạn; không cần Spark master/worker hay Kubernetes để thử các job local.

## Chuẩn bị

- Docker Engine/Docker Desktop đang chạy, Docker Compose v2 hỗ trợ `up --wait`.
- Trên Windows, bật Docker Desktop → Settings → Resources → WSL Integration cho distro đang dùng. Kiểm tra `docker compose version` và `docker info` trong WSL.
- Kích hoạt môi trường Python theo README, cài `requirements.txt`, chuẩn bị Java 17 cho Spark.
- Raw từ Drive đã nằm trong `data/raw/`. Các image dịch vụ tải riêng, không nằm trong gói dữ liệu.
- Nếu Kafka/HDFS/Elasticsearch local khác đang dùng cùng cổng, dừng dịch vụ đó trước hoặc điều chỉnh cổng và cấu hình client tương ứng.

Demo chỉ publish cổng vào `127.0.0.1`; Elasticsearch và HDFS tắt xác thực/quyền truy cập để thử trên máy cá nhân. Không dùng cấu hình này cho máy chủ công khai. Bản này không có cơ chế chịu lỗi nhiều node.

| Nhóm | Dịch vụ | Giới hạn RAM container |
| --- | --- | --- |
| `batch` | 1 NameNode + 1 DataNode, replication 1 | Tổng 2 GB |
| `streaming` | 1 Kafka broker kiêm KRaft controller, không cần ZooKeeper | 1 GB |
| `dashboard` | 1 Elasticsearch + 1 Kibana | Tổng 2 GB |

Đây là giới hạn cho container, không bao gồm Spark, Docker và hệ điều hành. Không cần bật cả ba nhóm cùng lúc. Nếu chỉ xử lý raw → Parquet local, không cần bật nhóm nào. Khi job hoặc dịch vụ bị OOM, kiểm tra `docker stats` và log rồi điều chỉnh `mem_limit`/heap; các mức trên là cấu hình khởi đầu, chưa phải kết quả benchmark.

## Bật và tắt từng nhóm

```bash
bash scripts/demo.sh config
bash scripts/demo.sh up batch
# Hoặc chọn nhóm cần dùng:
bash scripts/demo.sh up streaming
bash scripts/demo.sh up dashboard

bash scripts/demo.sh status
bash scripts/demo.sh logs
docker stats --no-stream

# Dừng nhóm khi không dùng; giữ nguyên volume:
bash scripts/demo.sh stop batch
bash scripts/demo.sh stop streaming
bash scripts/demo.sh stop dashboard
```

`up` đợi healthcheck thành công, rồi khởi tạo thư mục HDFS hoặc bốn Kafka topics tùy nhóm. Script dùng `--env-file /dev/null`, không tự nạp `.env` của repo vào Compose. Bật lại bằng cùng lệnh `up`; không chạy `down -v` vì tùy chọn đó xóa volume dữ liệu.

## Batch: dùng dữ liệu từ Drive

**Xử lý local:** không cần container, chạy trong môi trường Python tại gốc repo:

```bash
export PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 2g --conf spark.sql.shuffle.partitions=4 pyspark-shell'
unset HIVE_METASTORE_URI ELASTICSEARCH_HOSTS
python -m processing.batch.b2_clean_raw \
  --raw "file://$PWD/data/raw" \
  --clean "file://$PWD/data/lake/clean"
```

Hoặc chạy cả batch layer (B2 → B5 → B3 → B4 → B6 → B7 → B8) trên `data/raw`, kết quả ở `data/lake/`: `python pipeline.py batch`.

**Thử HDFS:** bật nhóm `batch`, nhập dữ liệu lần đầu khi ba thư mục đích chưa tồn tại:

```bash
docker compose --env-file /dev/null -f compose.demo.yaml exec -T namenode \
  hdfs dfs -put /input/steam_games_meta /input/steam_players_ccu /input/steam_reviews_raw /steam/raw/
docker compose --env-file /dev/null -f compose.demo.yaml exec -T namenode \
  hdfs dfs -ls /steam/raw/steam_reviews_raw
```

Raw được mount read-only vào `/input`. Nếu vừa tạo `data/raw/` sau khi container đã chạy, khởi động lại nhóm trước khi upload. Xem NameNode tại <http://localhost:9870>; báo cáo HDFS phải có `Live datanodes (1)`.

Để Spark trên host đọc block từ DataNode trong Docker, thêm dòng sau vào `/etc/hosts` của Linux/WSL đang chạy Spark (bằng trình soạn thảo có quyền quản trị):

```text
127.0.0.1 datanode
```

Địa chỉ NameNode thôi chưa đủ: Spark còn kết nối trực tiếp tới DataNode qua cổng 9866. Cấu hình client đã bật `dfs.client.use.datanode.hostname` để dùng hostname này thay vì IP nội bộ Docker.

```bash
export HADOOP_CONF_DIR="$PWD/infra/demo/hadoop"
export PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 2g --conf spark.sql.shuffle.partitions=4 --conf spark.hadoop.dfs.client.use.datanode.hostname=true pyspark-shell'
unset HIVE_METASTORE_URI ELASTICSEARCH_HOSTS
python -m processing.batch.b2_clean_raw \
  --raw hdfs://localhost:8020/steam/raw \
  --clean hdfs://localhost:8020/steam/clean
```

`--raw`/`--clean` là thư mục gốc: B2 đọc `steam_reviews_raw/`, `steam_players_ccu/`, `steam_games_meta/` và ghi `clean/reviews`, `clean/ccu`, `clean/games`. B2 tính lại toàn bộ từ raw rồi ghi đè, chạy lại không sinh trùng. Không chạy `storage/hdfs/setup_hdfs_dirs.sh` cho demo này vì script đó đặt replication 3. `infra/demo/hadoop/` chỉ là cấu hình client cho cụm Docker; không dùng nó để khởi động daemon Hadoop local.

## Streaming: Kafka một broker

`up streaming` tạo bốn topic `steam.reviews.raw`, `steam.players.ccu`, `steam.games.meta`, `steam.alerts`, mỗi topic một partition, replication factor 1. Metadata dùng compaction; các topic còn lại giữ dữ liệu 24 giờ.

Client chạy trên máy bạn dùng `localhost:9092`; client cùng mạng Compose dùng `kafka:19092`. Không dùng port-forward hoặc hostname `kafka` cho client trên host.

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 2g --conf spark.sql.shuffle.partitions=4 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell'
python -m processing.streaming.s1_review_metrics \
  --brokers localhost:9092 --checkpoint "file://$PWD/data/checkpoints/s1-demo"
```

Đưa một tập nhỏ review vào Kafka bằng producer của nhóm để kiểm tra. S1 ghi console; thêm `--es-nodes localhost:9200` (hoặc `WRITE_ES=1`) để ghi index `realtime_game_metrics`. Bật Kafka không tự phát lại file raw từ Drive; dùng `python pipeline.py replay` hoặc chạy S1 với `--source file` để đọc thẳng file raw. Khi chỉ thử S1, không cần bật HDFS. Các giới hạn logic B1/S1 trong README vẫn áp dụng.

## Dashboard: Elasticsearch và Kibana

Sau `up dashboard`, kích hoạt môi trường Python rồi chạy:

```bash
bash scripts/demo.sh init-es
curl -fsS http://localhost:9200/_cat/indices?v
```

Mở <http://localhost:5601>. Endpoint ES cho Python trên host là `http://localhost:9200`, không cần user/password trong demo. Script `init-es` bỏ thông tin đăng nhập mặc định từ `.env` và dùng mapping riêng ở `infra/demo/elasticsearch-indices.json`: một primary shard, **0 replica shard**. Một ES node không thể chứa cả primary và replica của cùng shard.

Mapping demo được sinh từ mapping nguồn `storage/elasticsearch/indices_mapping.json` (cùng 15 index, chỉ đổi thành 1 shard/0 replica); sửa mapping nguồn thì sinh lại file demo. Nếu index demo đã được tạo trước bằng mapping cũ (5 index), `init-es` giữ nguyên index đó; xóa index cũ trong demo rồi chạy lại `init-es`. Tạo index xong chưa có dữ liệu hoặc dashboard: cần chạy nhánh ghi ES của job và tạo data view trong Kibana theo phần việc của nhóm.

## Phạm vi và kiểm tra

Cấu hình này cung cấp Kafka, HDFS, Elasticsearch/Kibana; chưa bật crawler định kỳ, Hive, Prometheus/Grafana hoặc tự chạy Spark jobs. Chạy từng nhóm và kiểm tra đọc/ghi thực tế trước khi tích hợp. Cấu hình Kubernetes nhiều replica vẫn nằm trong `k8s/`; không chạy `scripts/deploy_all.sh` để khởi động demo Compose.

Nguồn cấu hình: [Apache Kafka Docker](https://kafka.apache.org/39/getting-started/docker/), [Hadoop image](https://hub.docker.com/r/apache/hadoop), [Hadoop image startup](https://raw.githubusercontent.com/apache/hadoop/docker-hadoop-runner/scripts/starter.sh), [Elasticsearch Docker](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docker.html).
