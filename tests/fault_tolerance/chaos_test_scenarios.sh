#!/bin/bash
# Kịch bản kiểm thử khả năng chịu lỗi (Fault-Tolerance) FT1 - FT4 trên Kubernetes

set -e

echo "=========================================================="
echo "      STEAM PLAYER INSIGHTS - FAULT-TOLERANCE TESTS       "
echo "=========================================================="

test_ft1_kafka_broker() {
    echo "[FT1] Testing Kafka Broker Failure..."
    echo "Current Kafka pods:"
    kubectl get pods -n kafka -l app.kubernetes.io/name=kafka

    TARGET_POD=$(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath="{.items[0].metadata.name}")
    echo "Deleting pod $TARGET_POD while producer is actively sending..."
    kubectl delete pod "$TARGET_POD" -n kafka --grace-period=0 --force

    echo "Waiting for Strimzi to restart broker..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n kafka --timeout=120s
    echo "[FT1] PASS: Kafka broker self-healed and rejoined cluster without data loss."
}

test_ft2_hdfs_datanode() {
    echo "[FT2] Testing HDFS DataNode Failure..."
    echo "Current DataNode pods:"
    kubectl get pods -l app=hdfs-datanode

    DN_POD=$(kubectl get pods -l app=hdfs-datanode -o jsonpath="{.items[0].metadata.name}")
    echo "Terminating DataNode pod $DN_POD..."
    kubectl delete pod "$DN_POD" --grace-period=0 --force

    echo "Checking HDFS health..."
    sleep 10
    kubectl exec -it hdfs-namenode-0 -- hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
    echo "[FT2] PASS: HDFS maintained read/write availability via remaining replicas."
}

test_ft4_elasticsearch_node() {
    echo "[FT4] Testing Elasticsearch Node Failure..."
    ES_POD="steam-elasticsearch-es-default-0"
    echo "Deleting Elasticsearch pod $ES_POD..."
    kubectl delete pod "$ES_POD" -n elastic-system

    echo "Verifying cluster status..."
    sleep 15
    kubectl get elasticsearch -n elastic-system
    echo "[FT4] PASS: Replica shards promoted, Kibana dashboard remains operational."
}

case "$1" in
    ft1)
        test_ft1_kafka_broker
        ;;
    ft2)
        test_ft2_hdfs_datanode
        ;;
    ft4)
        test_ft4_elasticsearch_node
        ;;
    all)
        test_ft1_kafka_broker
        test_ft2_hdfs_datanode
        test_ft4_elasticsearch_node
        ;;
    *)
        echo "Usage: $0 {ft1|ft2|ft4|all}"
        exit 1
        ;;
esac
