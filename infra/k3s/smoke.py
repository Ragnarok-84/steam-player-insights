"""Cross-node Kafka -> Spark/HDFS -> Elasticsearch integration check.

Uses a unique topic and HDFS directory per run; never touches crawled raw data.
"""
import json
import os
import uuid
from pathlib import Path

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession


def main():
    run_id = uuid.uuid4().hex
    topic = f"demo-smoke-{run_id}"
    marker = {"run_id": run_id, "appid": 570, "review": "good demo game"}
    broker = "kafka:9092"
    admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=30000)
    producer = consumer = spark = None
    try:
        replicas = int(os.getenv("SMOKE_REPLICATION_FACTOR", "1"))
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=replicas,
                                      topic_configs={"retention.ms": "3600000",
                                                     "min.insync.replicas": "2" if replicas == 3 else "1"})])
        producer = KafkaProducer(bootstrap_servers=broker, acks="all",
                                 value_serializer=lambda value: json.dumps(value).encode())
        producer.send(topic, marker).get(timeout=30)
        consumer = KafkaConsumer(topic, bootstrap_servers=broker, auto_offset_reset="earliest",
                                 enable_auto_commit=False, group_id=f"demo-{run_id}",
                                 consumer_timeout_ms=30000,
                                 value_deserializer=lambda value: json.loads(value.decode()))
        received = next(iter(consumer), None)
        assert received is not None and received.value == marker, "Kafka round-trip failed"
        print("PASS: Kafka produce/consume from compute node", flush=True)

        spark = (SparkSession.builder.appName("Steam_K3s_Smoke")
                 .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                 .config("spark.hadoop.dfs.replication", os.getenv("HDFS_REPLICATION", "1"))
                 .getOrCreate())
        path = f"hdfs://namenode:8020/steam/demo/{run_id}"
        spark.createDataFrame([received.value]).write.mode("errorifexists").parquet(path)
        rows = spark.read.parquet(path).collect()
        assert len(rows) == 1 and rows[0].asDict() == marker, "HDFS round-trip failed"
        print("PASS: Spark writes and reads Parquet across HDFS nodes", flush=True)

        es = Elasticsearch("http://elasticsearch:9200", request_timeout=30)
        try:
            mappings = json.loads(Path("infra/demo/elasticsearch-indices.json").read_text())
            for name, config in mappings.items():
                if not es.indices.exists(index=name):
                    es.indices.create(index=name, **config)
            index = "demo_smoke"
            if not es.indices.exists(index=index):
                es.indices.create(index=index, settings={"number_of_shards": 1, "number_of_replicas": 0})
            es.index(index=index, id=run_id, document=rows[0].asDict(), refresh="wait_for")
            assert es.get(index=index, id=run_id)["_source"] == marker, "ES round-trip failed"
            print("PASS: Elasticsearch write/read; demo mappings initialized", flush=True)
        finally:
            es.close()
        print(f"PASS: integration run {run_id}; HDFS output {path}", flush=True)
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.close(timeout=10)
        if spark is not None:
            spark.stop()
        try:
            admin.delete_topics([topic])
        finally:
            admin.close()


if __name__ == "__main__":
    main()
