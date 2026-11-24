set -euo pipefail
k() { sudo k3s kubectl "$@"; }
for i in $(seq 1 120); do
  if k -n steam-demo exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null | grep -qx 'steam.reviews.raw'; then break; fi
  sleep 4
done
k -n steam-demo exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
test -d ~/phase2-data/raw/steam_reviews_raw
cat <<'YAML' | k apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: phase2-replay-1000
  namespace: steam-demo
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 1800
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: steam-server
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
      - name: replay
        image: python:3.11-slim-bookworm
        workingDir: /input/code
        command: [bash, -ec]
        args:
        - |
          pip install --no-cache-dir 'kafka-python==3.0.11' 'python-dotenv==1.2.3'
          python -B -m crawlers.replay_jsonl_to_kafka --raw /input/raw --rate 10000
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: kafka:9092
        - name: PYTHONUNBUFFERED
          value: "1"
        - name: PYTHONDONTWRITEBYTECODE
          value: "1"
        volumeMounts:
        - {name: code, mountPath: /input/code, readOnly: true}
        - {name: raw, mountPath: /input/raw, readOnly: true}
        resources:
          requests: {cpu: 400m, memory: 192Mi}
          limits: {cpu: "1", memory: 512Mi}
      volumes:
      - name: code
        hostPath: {path: /home/steam/phase2-repo, type: Directory}
      - name: raw
        hostPath: {path: /home/steam/phase2-data/raw, type: Directory}
YAML
k -n steam-demo logs -f job/phase2-replay-1000 --pod-running-timeout=600s 2>&1 | tee ~/phase2-replay.log
k -n steam-demo wait --for=condition=Complete job/phase2-replay-1000 --timeout=90s