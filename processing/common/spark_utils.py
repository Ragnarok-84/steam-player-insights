import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = os.getenv("HDFS_RAW_PATH", "/steam/raw")
CLEAN_PATH = os.getenv("HDFS_CLEAN_PATH", "/steam/clean")
CURATED_PATH = os.getenv("HDFS_CURATED_PATH", "/steam/curated")
CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "/steam/checkpoints")

# Thư mục con theo topic; trùng với output của crawlers/collect_local.py (JsonlProducer)
# nên dữ liệu crawl local có thể dùng trực tiếp làm vùng raw.
TOPIC_DIRS = {
    "steam.reviews.raw": "steam_reviews_raw",
    "steam.players.ccu": "steam_players_ccu",
    "steam.games.meta": "steam_games_meta",
}


def get_spark_session(app_name: str = "SteamDataPipeline", master: str = None) -> SparkSession:
    """
    Tạo SparkSession dùng chung.
    - HDFS_NAMENODE_URL -> fs.defaultFS (để trống khi chạy local với đường dẫn file://)
    - SPARK_PACKAGES -> spark.jars.packages (vd. Kafka connector cho B1/streaming)
    - HIVE_METASTORE_URI -> bật Hive catalog
    """
    builder = SparkSession.builder.appName(app_name)

    master = master or os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)

    namenode = os.getenv("HDFS_NAMENODE_URL")
    if namenode:
        builder = builder.config("spark.hadoop.fs.defaultFS", namenode)

    packages = os.getenv("SPARK_PACKAGES")
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    builder = (
        builder.config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        # Số partition shuffle/state store; streaming cố định giá trị này theo checkpoint, đổi thì xoá checkpoint
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )

    hive_uri = os.getenv("HIVE_METASTORE_URI")
    if hive_uri:
        builder = (
            builder.config("hive.metastore.uris", hive_uri)
            .config("spark.sql.catalogImplementation", "hive")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _fs_for(spark, path: str):
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark._jsc.hadoopConfiguration()), hpath


def path_exists(spark, path: str) -> bool:
    """Kiểm tra đường dẫn (hỗ trợ glob) qua Hadoop FileSystem, đúng với mọi scheme: hdfs://, file://, không scheme."""
    fs, hpath = _fs_for(spark, path)
    matches = fs.globStatus(hpath)
    return matches is not None and len(matches) > 0


def existing_paths(spark, patterns):
    return [p for p in patterns if path_exists(spark, p)]


def raw_topic_paths(spark, raw_root: str, topic: str):
    """
    Các thư mục raw của một topic: {raw}/{dir} và {raw}/*/{dir}
    (thư mục phụ như data/raw/retry-reviews/steam_reviews_raw do collection_guide tạo ra).
    """
    sub = TOPIC_DIRS[topic]
    return existing_paths(spark, [f"{raw_root}/{sub}", f"{raw_root}/*/{sub}"])


def read_raw_json_lines(spark, raw_root: str, topic: str):
    """Đọc toàn bộ JSON Lines (gzip hoặc không) của một topic thành DataFrame cột `value`. File ẩn/.tmp bị bỏ qua."""
    paths = raw_topic_paths(spark, raw_root, topic)
    if not paths:
        return None
    return spark.read.option("recursiveFileLookup", "true").text(paths)


def write_table(spark, df, path: str, table: str = None, partition_by=None, mode: str = "overwrite"):
    """Ghi Parquet; nếu có Hive Metastore thì đăng ký bảng external trỏ tới path."""
    writer = df.write.mode(mode).option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    if table and os.getenv("HIVE_METASTORE_URI"):
        spark.sql("CREATE DATABASE IF NOT EXISTS steam_analytics")
        spark.sql(f"DROP TABLE IF EXISTS steam_analytics.{table}")
        spark.catalog.createTable(f"steam_analytics.{table}", path=path, source="parquet")
        if partition_by:
            spark.sql(f"MSCK REPAIR TABLE steam_analytics.{table}")


def read_stream(spark, topic: str, source: str, brokers: str = None, raw_root: str = None,
                max_files_per_trigger: int = 1, starting_offsets: str = "latest"):
    """
    Nguồn streaming trả về DataFrame có cột `value` (JSON string).
    - source=kafka: đọc topic Kafka (cần SPARK_PACKAGES chứa spark-sql-kafka).
    - source=file: đọc dần các file JSONL trong vùng raw (vd. dữ liệu crawl local), mô phỏng luồng khi chưa có Kafka.
    """
    if source == "kafka":
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", brokers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
        )
    path = f"{raw_root or RAW_PATH}/{TOPIC_DIRS[topic]}"
    return (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("recursiveFileLookup", "true")
        .load(path)
        .select("value")
    )


def add_stream_args(parser):
    parser.add_argument("--source", choices=["kafka", "file"], default=os.getenv("STREAM_SOURCE", "kafka"))
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--raw", default=RAW_PATH, help="Raw root khi --source file")
    parser.add_argument("--max-files", type=int, default=1, help="Số file mỗi micro-batch khi --source file")
    parser.add_argument("--starting-offsets", default="latest", help="Kafka: latest | earliest")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--es-nodes", default=None)
    parser.add_argument("--curated", default=CURATED_PATH)
    return parser
