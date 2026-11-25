# Phase 3 trên Hyper-V

Dùng bốn VM Ubuntu của dự án. Triển khai overlay `k8s/phase3` vào `steam-ft` theo [FAULT_TEST.md](../../infra/k3s/FAULT_TEST.md). Overlay đặt Kibana về 0 replica trong lúc đo. Tạm dừng các dịch vụ `steam-demo` theo hướng dẫn và giữ PVC để đối chiếu Phase 2. Giữ nhãn Kafka/HDFS theo hướng dẫn. Gắn thêm `steam.insights/compute=true` cho worker 1, 2 và 3.

## Image và dữ liệu

Build `containers/Dockerfile.spark` từ [main 1d45953](https://github.com/Ragnarok-84/steam-player-insights/commit/1d459539f58f058a2698f02a6d8bcee5e1565c31). Image dùng `/opt/spark/work-dir` và cần thêm `COPY infra/ infra/` để có executor pod template. Dùng cùng archive để import `localhost/steam-spark:k3s-20261124-1d45953` vào containerd trên cả bốn VM.

Dùng `tests/scalability/make_scale_subsets.py` với snapshot crawl và `data/target_apps.json` để tạo `raw_100`, `raw_500` và `raw_1000`. Đưa từng thư mục vào `/steam/bench` trên HDFS. Driver stream dùng bản đọc tại `/var/lib/steam-phase3/subsets/raw_<scale>` trên VM driver để phát dữ liệu vào Kafka. Kafka có ba partition, replication factor 3 và min ISR 2. HDFS có replication 2. Kiểm tra ISR và HDFS trước khi đo.

## Batch và kiểm tra executor

Tạo thư mục kết quả trên VM driver với UID 185. Dùng tên khác nhau cho từng cấu hình.

```bash
image=localhost/steam-spark:k3s-20261124-1d45953
sha=1d459539f58f058a2698f02a6d8bcee5e1565c31
name=p3-pilot-batch-e1

# Trên VM chạy driver
sudo install -d -o 185 -m 755 "/var/lib/steam-phase3/runs/$name"

# Trên máy có kubectl của cụm
python3 infra/k3s/render_phase3.py batch --name "$name" --image "$image" \
  --revision "$sha" --scale 100 --executors 1 > "$name.json"
kubectl create -f "$name.json"
kubectl -n steam-ft get pods -l "steam-benchmark=$name" -o wide
kubectl get pods -A -o wide
kubectl -n steam-ft top pods -l "steam-benchmark=$name"
```

`probe` chờ đúng số executor đăng ký với Spark rồi đếm review thật từ HDFS. `batch` chạy B2, B5, B3, B4, B6, B7, B8, B9 bằng bộ đo trong repo. Dùng `--runs 3` cho ma trận. Mỗi executor dùng một core, heap 1 GiB và overhead 512 MiB. Giữ 16 shuffle partition giữa các cấu hình. Đo từng lần để các driver và executor không tranh RAM hay CPU. Mỗi Job ghi vào thư mục HDFS và thư mục bằng chứng theo tên Job.

## Streaming

Khởi tạo mapping trên Elasticsearch của `steam-ft` trước khi khởi động stream. Job tạo các index còn thiếu và giữ dữ liệu đã có.

```bash
kubectl apply -f k8s/phase3/jobs/es-init.yaml
kubectl -n steam-ft wait --for=condition=complete job/phase3-es-init --timeout=180s
```

Render với mode `s1`, `s2` hoặc `s3`. Với S2/S3, truyền `--curated hdfs://namenode:8020/steam/bench/runs/<batch-job>/r1/curated` trỏ đến baseline từ B7 cùng quy mô. Tạo thư mục kết quả bằng cùng lệnh phía trên. Khi driver đã tạo `progress.jsonl` và pod executor đã Ready, chạy bộ phát tải và đợi các micro-batch hoàn thành.

```bash
pod=$(kubectl -n steam-ft get pod -l "job-name=$name" -o jsonpath='{.items[0].metadata.name}')
output=/opt/spark/work-dir/tests/results
kubectl -n steam-ft exec "$pod" -c producer -- python3 tests/scalability/sc3_streaming_load_test.py \
  --no-start-job --job s1 --scale 100 --env k3s --executors 1 \
  --dataset steam_1000g_20261118 --raw /bench/raw --brokers kafka:9092 --es \
  --rates 200 --step-seconds 90 --producers 1 \
  --metrics-file "$output/progress.jsonl" --latency-file "$output/latency.jsonl"
```

Container producer có ngân sách CPU/RAM riêng và chia sẻ thư mục kết quả với driver. Lưu log driver, pod đang chạy, manifest, metrics và CSV từ thư mục kết quả của VM trước khi dừng Job stream. Xác nhận từng lượt có micro-batch, latency và consumer lag. Với ma trận, chạy hai bậc 1.000 và 5.000 bản ghi/giây, mỗi bậc 60 giây, như [báo cáo đo local](../../docs/reports/streaming_es_benchmark_scale_20261123.md). Dùng hai tiến trình producer và ghi lại tốc độ gửi thực tế cùng tài nguyên của pod. Dùng checkpoint mới cho từng lượt. Chạy `python3 tests/results_log.py check <CSV common>` và kiểm tra thêm các dòng có `status=ok`, đủ job, executor và repeat.

Chạy FT1, FT2, FT3 và FT5 theo [FAULT_TEST.md](../../infra/k3s/FAULT_TEST.md). Dùng `jobs/ft-watch.yaml` cho FT1, FT2 và FT5. Observer ở worker 3 và cài gói WebHDFS cùng phiên bản trong `requirements.txt` trước khi báo `READY`. FT4 dùng `ft4-es.yaml` trong namespace `steam-ft-es` sau cùng, khi Spark đã dừng. Ba pod ES nằm trên ba worker riêng. Hai node `master,data,ingest` chứa shard và một node `master,voting_only` giữ quorum. Dùng dữ liệu thử và index có một replica. Lưu shard placement và số document trước khi xóa một data pod, lúc pod vắng và khi cluster về green.
