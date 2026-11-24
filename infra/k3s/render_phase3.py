"""Render a native Spark job for the steam-ft benchmark on four Hyper-V VMs."""

import argparse
import json
from pathlib import PurePosixPath
import re


NAMESPACE = "steam-ft"
IMAGE_ROOT = "/opt/spark/work-dir"
RESULTS = f"{IMAGE_ROOT}/tests/results"
OUTPUT = "/var/lib/steam-phase3/runs"
DATA = "/var/lib/steam-phase3/subsets"
HDFS = "hdfs://namenode:8020/steam/bench"
DATASET = "steam_1000g_20261118"


def env_field(name, field):
    return {"name": name, "valueFrom": {"fieldRef": {"fieldPath": field}}}


def render(args):
    name = args.name
    lake = f"{HDFS}/runs/{name}"
    raw = f"{HDFS}/raw_{args.scale}"
    conf = {
        "spark.kubernetes.namespace": NAMESPACE,
        "spark.kubernetes.authenticate.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "spark.kubernetes.authenticate.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        "spark.kubernetes.container.image": args.image,
        "spark.kubernetes.container.image.pullPolicy": "Never",
        "spark.kubernetes.driver.pod.name": "$(POD_NAME)",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "7078",
        "spark.blockManager.port": "7079",
        "spark.kubernetes.executor.node.selector.steam.insights/compute": "true",
        "spark.kubernetes.executor.podTemplateFile": f"{IMAGE_ROOT}/infra/k3s/executor-template.yaml",
        "spark.kubernetes.executor.podNamePrefix": "$(POD_NAME)",
        "spark.kubernetes.executor.label.steam-benchmark": name,
        "spark.kubernetes.executor.request.cores": "750m",
        "spark.kubernetes.executor.limit.cores": "1",
        "spark.executor.instances": str(args.executors),
        "spark.executor.cores": "1",
        "spark.executor.memory": "1g",
        "spark.executor.memoryOverhead": "512m",
        "spark.dynamicAllocation.enabled": "false",
        "spark.hadoop.dfs.client.use.datanode.hostname": "true",
        "spark.hadoop.dfs.replication": "2",
        "spark.executorEnv.ELASTICSEARCH_HOSTS": "http://elasticsearch:9200",
        "spark.executorEnv.HADOOP_USER_NAME": "hdfs",
    }
    values = {
        "SPARK_MASTER": "k8s://https://kubernetes.default.svc:443",
        "SPARK_EXTRA_CONF": ";".join(f"{key}={value}" for key, value in conf.items()),
        "SPARK_DRIVER_MEMORY": "1536m",
        "SPARK_SHUFFLE_PARTITIONS": "16",
        "PYTHONUNBUFFERED": "1",
        "ELASTICSEARCH_HOSTS": "http://elasticsearch:9200",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "HDFS_NAMENODE_URL": "hdfs://namenode:8020",
        "HADOOP_USER_NAME": "hdfs",
        "HDFS_RAW_PATH": raw,
        "HDFS_CLEAN_PATH": f"{lake}/clean",
        "HDFS_CURATED_PATH": f"{lake}/curated",
        "HDFS_CHECKPOINT_PATH": f"{lake}/checkpoints",
        "GIT_COMMIT": args.revision,
    }
    mode = args.mode
    mounts = [{"name": "results", "mountPath": RESULTS}]
    volumes = [{"name": "results", "hostPath": {"path": f"{OUTPUT}/{name}", "type": "Directory"}}]
    streaming = mode in ("s1", "s2", "s3")
    if mode == "batch":
        command = [
            "python3", "tests/scalability/sc1_sc2_batch_benchmark.py",
            "--profile", "k3s", "--scales", str(args.scale), "--executors", str(args.executors),
            "--runs", str(args.runs), "--raw-template", f"{HDFS}/raw_{{scale}}",
            "--lake-template", lake + "/r{run}", "--image-tag", args.image,
            "--topology", args.topology, "--shuffle-partitions", "16",
            "--log-dir", f"{RESULTS}/batch-logs", "--log-common", "--dataset", DATASET,
        ]
    elif mode == "probe":
        command = ["python3", "-c", "\n".join((
            "import os",
            "from processing.common.spark_utils import get_spark_session, read_raw_json_lines",
            "from tests.scalability.sc1_sc2_batch_benchmark import _wait_executors",
            "spark = get_spark_session('phase3-executor-probe')",
            "try:",
            f"    _wait_executors(spark, {args.executors}, 180)",
            "    sample = read_raw_json_lines(spark, os.environ['HDFS_RAW_PATH'], 'steam.reviews.raw')",
            "    review_count = sample.count()",
            "    if review_count <= 0:",
            "        raise RuntimeError('empty crawl dataset')",
            f"    print('PASS: {args.executors} registered executors; reviews=' + str(review_count), flush=True)",
            "finally:",
            "    spark.stop()",
        ))]
    else:
        values["HDFS_CURATED_PATH"] = args.curated or values["HDFS_CURATED_PATH"]
        command = [
            "python3", "pipeline.py", mode, "--profile", "cluster", "--es", "--",
            "--source", "kafka", "--starting-offsets", "latest", "--trigger", "10 seconds",
            "--checkpoint", f"{lake}/checkpoints", "--metrics-file", f"{RESULTS}/progress.jsonl",
            "--latency-file", f"{RESULTS}/latency.jsonl",
        ]
        volumes.append({"name": "raw", "hostPath": {"path": f"{DATA}/raw_{args.scale}", "type": "Directory"}})
    container = {
        "name": "driver", "image": args.image, "imagePullPolicy": "Never",
        "workingDir": IMAGE_ROOT, "command": command, "volumeMounts": mounts,
        "env": [
            env_field("POD_NAME", "metadata.name"), env_field("SPARK_DRIVER_HOST", "status.podIP"),
            *({"name": key, "value": value} for key, value in values.items()),
        ],
        "resources": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "2432Mi"},
        },
    }
    containers = [container]
    if streaming:
        containers.append({
            "name": "producer", "image": args.image, "imagePullPolicy": "Never",
            "workingDir": IMAGE_ROOT, "command": ["sleep", "infinity"],
            "env": [{"name": "GIT_COMMIT", "value": args.revision}],
            "volumeMounts": [*mounts, {"name": "raw", "mountPath": "/bench/raw", "readOnly": True}],
            "resources": {
                "requests": {"cpu": "500m", "memory": "128Mi"},
                "limits": {"cpu": "2", "memory": "768Mi"},
            },
        })
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "backoffLimit": 0, "activeDeadlineSeconds": args.deadline,
            "template": {
                "metadata": {
                    "labels": {"spark-role": "driver", "steam-benchmark": name},
                    "annotations": {"kubectl.kubernetes.io/default-container": "driver"},
                },
                "spec": {
                    "serviceAccountName": "spark-driver", "restartPolicy": "Never",
                    "nodeSelector": {"kubernetes.io/hostname": args.driver_node},
                    "containers": containers, "volumes": volumes,
                },
            },
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("batch", "probe", "s1", "s2", "s3"))
    parser.add_argument("--name", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--topology", default="Hyper-V/steam-ft")
    parser.add_argument("--driver-node", default="steam-worker-3")
    parser.add_argument("--executors", type=int, choices=(1, 2, 4), required=True)
    parser.add_argument("--scale", type=int, choices=(100, 500, 1000), default=100)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--deadline", type=int, default=14400)
    parser.add_argument("--curated", help="HDFS path to B7 baseline output for S2/S3")
    args = parser.parse_args()
    if not re.fullmatch(r"p3-[a-z0-9]+(?:-[a-z0-9]+)*", args.name) or len(args.name) > 35:
        parser.error("--name must start with p3- and use lowercase letters, numbers and hyphens (max 35)")
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("--revision must be a full Git SHA")
    if not re.fullmatch(r"localhost/steam-spark:[a-z0-9][a-z0-9.-]*", args.image):
        parser.error("--image must name a tagged localhost/steam-spark image")
    if min(args.runs, args.deadline) < 1:
        parser.error("--runs and --deadline must be positive")
    if args.mode in ("s2", "s3") and not args.curated:
        parser.error("S2/S3 require --curated pointing to the B7 baseline")
    if args.curated and (
        not args.curated.startswith(f"{HDFS}/runs/") or ".." in PurePosixPath(args.curated).parts
    ):
        parser.error("--curated must be inside the benchmark runs directory")
    print(json.dumps(render(args), indent=2))


if __name__ == "__main__":
    main()
