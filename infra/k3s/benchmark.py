"""A fixed distributed shuffle workload; emits measured results as JSON."""
import argparse
import json
import time

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--executors", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("rows must be positive")
    startup = time.monotonic()
    spark = SparkSession.builder.appName("Steam_Distributed_Benchmark").getOrCreate()
    try:
        deadline = time.monotonic() + 180
        # StatusTracker includes the driver; require the requested executor count.
        while len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) < args.executors + 1:
            if time.monotonic() >= deadline:
                raise RuntimeError("Requested executors did not register; inspect Pending/OOM/image errors")
            time.sleep(2)
        registered = time.monotonic()
        # Keep input size and partition count identical across 1/2/4-executor runs.
        frame = spark.range(args.rows, numPartitions=16).withColumn("bucket", F.col("id") % 1000)
        result = frame.groupBy("bucket").agg(F.count("id").alias("records"), F.sum("id").alias("id_sum"))
        result.write.mode("errorifexists").parquet(args.output)
        summary = spark.read.parquet(args.output).agg(F.sum("records"), F.sum("id_sum")).first()
        assert summary[0] == args.rows
        assert summary[1] == args.rows * (args.rows - 1) // 2
        print(json.dumps({"status": "PASS", "executors": args.executors, "rows": args.rows,
                          "startup_seconds": round(registered - startup, 3),
                          "workload_seconds": round(time.monotonic() - registered, 3),
                          "output": args.output}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
