"""Render a native Spark-on-Kubernetes client-mode Job without a Spark operator."""
import argparse
import json
import uuid


def render(executors, rows):
    run_id = uuid.uuid4().hex[:12]
    name = f"ft-bench-{executors}-{run_id}"
    command = """exec spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode client \
  --conf spark.kubernetes.namespace=steam-ft \
  --conf spark.kubernetes.authenticate.oauthTokenFile=/var/run/secrets/kubernetes.io/serviceaccount/token \
  --conf spark.kubernetes.authenticate.caCertFile=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
  --conf spark.kubernetes.container.image=localhost/steam-spark-demo:k3s-20261026 \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.driver.pod.name="$POD_NAME" \
  --conf spark.driver.host="$POD_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.kubernetes.executor.node.selector.steam.insights/compute=true \
  --conf spark.kubernetes.executor.podTemplateFile=/app/infra/k3s/executor-template.yaml \
  --conf spark.kubernetes.executor.request.cores=500m \
  --conf spark.kubernetes.executor.limit.cores=1 \
  --conf spark.executor.instances="$EXECUTORS" \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  --conf spark.kubernetes.executor.podNamePrefix="$POD_NAME" \
  /app/infra/k3s/benchmark.py --executors "$EXECUTORS" --rows "$ROWS" \
  --output "hdfs://namenode:8020/steam/bench/$POD_NAME"
"""
    container = {"name": "driver", "image": "localhost/steam-spark-demo:k3s-20261026",
                 "imagePullPolicy": "Never", "command": ["bash", "-ec", command],
                 "env": [{"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                         {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                         {"name": "EXECUTORS", "value": str(executors)}, {"name": "ROWS", "value": str(rows)}],
                 "resources": {"requests": {"cpu": "500m", "memory": "1536Mi"},
                               "limits": {"cpu": "1", "memory": "2Gi"}}}
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": "steam-ft"},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 1800,
                     "template": {"metadata": {"labels": {"spark-role": "driver", "app": name}},
                                  "spec": {"serviceAccountName": "spark-driver", "restartPolicy": "Never",
                                           "nodeSelector": {"steam.insights/compute": "true"},
                                           "containers": [container]}}}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executors", type=int, choices=[1, 2, 4], required=True)
    parser.add_argument("--rows", type=int, default=1000000)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("rows must be positive")
    print(json.dumps(render(args.executors, args.rows), indent=2))
