import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "SteamDataPipeline", master: str = None) -> SparkSession:
    """
    Tạo hoặc lấy SparkSession cấu hình chuẩn hỗ trợ Kafka, HDFS và Elasticsearch.
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Cấu hình nạp package Kafka và Elasticsearch cho Spark nếu cần
    builder = (
        builder.config("spark.sql.streaming.checkpointLocation", os.getenv("HDFS_CHECKPOINT_PATH", "/steam/checkpoints"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # Cấu hình Hive Metastore nếu có biến môi trường
    hive_uri = os.getenv("HIVE_METASTORE_URI")
    if hive_uri:
        builder = (
            builder.config("hive.metastore.uris", hive_uri)
            .config("spark.sql.catalogImplementation", "hive")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
