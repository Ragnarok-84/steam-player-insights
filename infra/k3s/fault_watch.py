"""Observe acknowledged Kafka records and a pre-replicated HDFS file during failures.

The script never stops nodes or deletes pods. Inject failure from another terminal.
"""
import argparse
import json
import time
import uuid

from hdfs import InsecureClient
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args()
    if args.seconds < 60:
        parser.error("Use at least 60 seconds to inject a failure")
    run_id = uuid.uuid4().hex
    topic = f"ft-watch-{run_id}"
    admin = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=30000)
    admin.create_topics([NewTopic(topic, 3, 3, topic_configs={"min.insync.replicas": "2",
                                                           "retention.ms": "86400000"})])
    producer = KafkaProducer(bootstrap_servers="kafka:9092", acks="all", retries=3,
                             request_timeout_ms=10000, max_block_ms=10000,
                             value_serializer=lambda value: json.dumps(value).encode())
    consumer = KafkaConsumer(topic, bootstrap_servers="kafka:9092", auto_offset_reset="earliest",
                             enable_auto_commit=False, group_id=f"ft-{run_id}",
                             value_deserializer=lambda value: json.loads(value.decode()))
    hdfs = InsecureClient("http://namenode:9870", user="root", timeout=10)
    acked, seen = set(), set()
    attempts = kafka_errors = hdfs_errors = hdfs_success = 0
    longest_kafka_gap = longest_hdfs_gap = 0.0
    last_kafka = last_hdfs = time.monotonic()
    path = "/steam/ft-probe/marker.txt"
    try:
        with hdfs.read(path) as stream:
            assert stream.read() == b"steam-ft-probe\n", "Prepare the RF2 probe before starting"
        print(json.dumps({"event": "READY", "topic": topic, "seconds": args.seconds,
                          "hdfs_probe": path}), flush=True)
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            seq = attempts
            attempts += 1
            try:
                producer.send(topic, {"run_id": run_id, "seq": seq}).get(timeout=15)
                acked.add(seq)
                now = time.monotonic()
                longest_kafka_gap = max(longest_kafka_gap, now - last_kafka)
                last_kafka = now
            except Exception as exc:
                kafka_errors += 1
                print(json.dumps({"event": "kafka_error", "seq": seq, "error": str(exc)}), flush=True)
            try:
                with hdfs.read(path) as stream:
                    assert stream.read() == b"steam-ft-probe\n", "HDFS content changed"
                hdfs_success += 1
                now = time.monotonic()
                longest_hdfs_gap = max(longest_hdfs_gap, now - last_hdfs)
                last_hdfs = now
            except Exception as exc:
                hdfs_errors += 1
                print(json.dumps({"event": "hdfs_error", "error": str(exc)}), flush=True)
            time.sleep(1)
        observation_end = time.monotonic()
        # Wait for consumer assignment and drain all partitions; check every acked ID.
        drain_deadline = time.monotonic() + 60
        while time.monotonic() < drain_deadline:
            for messages in consumer.poll(timeout_ms=1000).values():
                for record in messages:
                    if record.value.get("run_id") == run_id:
                        seen.add(record.value["seq"])
            if acked and acked <= seen:
                break
        missing = sorted(acked - seen)
        result = {"event": "RESULT", "run_id": run_id, "topic": topic, "attempts": attempts,
                  "acknowledged": len(acked), "received_unique": len(seen), "missing_acked": missing,
                  "kafka_errors": kafka_errors, "hdfs_reads": hdfs_success, "hdfs_errors": hdfs_errors,
                  "longest_kafka_success_gap_seconds": round(max(longest_kafka_gap, observation_end - last_kafka), 3),
                  "longest_hdfs_success_gap_seconds": round(max(longest_hdfs_gap, observation_end - last_hdfs), 3)}
        print(json.dumps(result), flush=True)
        if missing or not acked or not hdfs_success:
            raise RuntimeError("Integrity/availability observation failed; inspect the JSON result")
        print("INTEGRITY_OK: all acknowledged records were read; inspect gaps/errors and failure timestamps separately", flush=True)
    finally:
        consumer.close()
        producer.close(timeout=10)
        admin.close()


if __name__ == "__main__":
    main()
