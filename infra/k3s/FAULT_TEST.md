# Kiểm thử chịu lỗi và mở rộng lên bốn/năm máy

Thực hiện sau khi cụm ba VM và bài smoke trong [README](README.md) hoạt động. Repo cung cấp manifest/script để chạy, chưa có kết quả kiểm thử trên cụm thật.

## 1. Tạo môi trường kiểm thử riêng

`k8s/fault-test/` dùng namespace **steam-ft** và PVC mới, không ghi đè **steam-demo**. Kafka 3.9 của demo được format với quorum KRaft tĩnh một controller; không đổi trực tiếp `replicas=3` rồi thay voters trên volume cũ. Bản kiểm thử tạo một quorum ba controller/broker ngay từ đầu. Kafka cũ và dữ liệu HDFS demo giữ lại để đối chiếu; nếu cần dữ liệu thật trong steam-ft, nhập lại từ bản raw trên Drive.

Trước khi triển khai, hoàn tất các Job demo đang chạy rồi tạm dừng các dịch vụ demo để nhường RAM (giữ PVC):

```bash
kubectl -n steam-demo get jobs,pods
kubectl -n steam-demo scale statefulset kafka namenode datanode elasticsearch --replicas=0
kubectl -n steam-demo scale deployment kibana --replicas=0
kubectl -n steam-demo get pods
```

Đợi các pod dịch vụ demo dừng trước khi deploy. Không chạy `scripts/k3s-demo.sh deploy` trong lúc steam-ft đang hoạt động vì lệnh đó sẽ bật lại bản demo.

```bash
bash scripts/k3s-fault-test.sh label-nodes steam-server steam-worker-1 steam-worker-2
bash scripts/k3s-fault-test.sh render > /tmp/steam-ft.yaml
kubectl apply --dry-run=server -k k8s/fault-test
bash scripts/k3s-fault-test.sh deploy
bash scripts/k3s-fault-test.sh status
```

Kafka có ba broker kiêm controller, dùng ID 1/2/3 và địa chỉ riêng theo ordinal pod. StatefulSet dùng `Parallel` để khởi động quorum đồng thời; anti-affinity bắt buộc đặt ba broker trên ba node khác nhau. Topics có ba partition, RF=3, min ISR=2. Producer kiểm thử dùng `acks=all`.

HDFS có một NameNode và hai DataNode trên server/worker 1 (nhãn `ft-hdfs=true`), replication mặc định 2. Anti-affinity bắt buộc giữ hai DataNode trên hai node khác nhau. Mỗi DataNode quảng bá hostname riêng, không cùng quảng bá `datanode-0`. Readiness DataNode kiểm tra chính daemon của pod, không đòi cả hai node còn sống khi một node hỏng.

Trước kiểm thử, cần thấy Kafka ISR đủ ba ID cho mọi partition, quorum có leader và ba voters; HDFS có `Live datanodes (2)`. Nhìn cột NODE của `kubectl get pods -o wide` để biết chính xác pod nằm trên máy nào.

Quota storage là 100 GiB để chứa NameNode 10 GiB, hai DataNode tổng 20 GiB, ba Kafka tổng 30 GiB và ES 10 GiB. Đây là PVC request; local-path không áp dung lượng đĩa cứng. Theo dõi dung lượng cả volume demo đang giữ lại lẫn volume kiểm thử. Giới hạn RAM dịch vụ tổng khoảng 8,5 GiB, không tính k3s/OS, observer hoặc Spark. Phải kiểm tra allocatable theo từng node, không chỉ cộng RAM toàn cụm.

Elasticsearch vẫn một node và NameNode vẫn một instance: không kiểm thử HA cho hai thành phần này. Cụm cũng vẫn chỉ có một k3s server. Không tắt server trong kịch bản chịu lỗi worker.

## 2. Build image kiểm thử

Môi trường kiểm thử dùng chung image với demo. Nếu đã build theo [README mục 5](README.md#5-image-spark-và-bài-kiểm-tra-luồng-dữ-liệu) thì chỉ cần import vào node mới; nếu chưa, build một lần trên compute rồi import vào **mọi node mang nhãn compute**:

```bash
podman build -t localhost/steam-spark-demo:k3s-20261026 \
  --ignorefile infra/k3s/containerignore -f infra/k3s/Dockerfile.spark-demo .
podman save --format docker-archive -o /tmp/steam-spark-ft.tar \
  localhost/steam-spark-demo:k3s-20261026
sudo k3s ctr images import /tmp/steam-spark-ft.tar
```

Chép file tar sang worker mới và import ở đó, không build lại riêng từng máy. Image bổ sung entrypoint executor cho Spark 3.5.7, benchmark và observer; `imagePullPolicy: Never` sẽ báo lỗi rõ nếu máy chưa có image. Image cũ `conda-20261007`/`k3s-20261008` chứa code xử lý trước bản merge 26/10 (B2 nhận tham số kiểu cũ), không dùng lại.

Trên server, chạy smoke cho môi trường mới:

```bash
bash scripts/k3s-fault-test.sh smoke
kubectl -n steam-ft logs -f job/ft-smoke
kubectl -n steam-ft wait --for=condition=complete job/ft-smoke --timeout=600s
```

Smoke dùng topic RF=3 và Parquet replication=2 nhưng không tự gây lỗi. Không dùng smoke thành bằng chứng chịu lỗi.

## 3. Kiểm thử dữ liệu khi pod/node hỏng

Chuẩn bị file HDFS riêng và **đợi đủ hai bản sao thực tế** trước khi gây lỗi:

```bash
bash scripts/k3s-fault-test.sh prepare-probe
```

Kiểm tra `fsck` có `Live_repl=2`, hai địa chỉ DataNode khác nhau và trạng thái HEALTHY. `dfs.replication=2` hoặc metadata replication=2 thôi chưa chứng minh hai bản sao đã ghi xong. Khi cần tăng replication cho dữ liệu đã nhập trước đó trong steam-ft, chạy `hdfs dfs -setrep -w -R 2 DUONG_DAN` qua NameNode; thay mặc định không thay replication của file cũ.

Khởi động observer năm phút trên node compute ổn định:

```bash
bash scripts/k3s-fault-test.sh watch
kubectl -n steam-ft logs -f job/ft-watch
```

Đợi log `READY`. Observer gửi record ID riêng vào topic ba partition/RF=3/min ISR=2, ghi nhận những lần Kafka đã ACK, đọc đi đọc lại file HDFS và cuối buổi đọc lại tất cả record đã ACK. Topic/log được giữ để kiểm tra, không tự xóa pod/node.

**Lượt A — pod restart:** từ terminal khác, chọn đúng một broker và một DataNode, thử từng cái một:

```bash
kubectl -n steam-ft get pods -o wide
kubectl -n steam-ft delete pod kafka-1
# Đợi pod quay lại Ready và ISR đủ 3, rồi mới thử DataNode:
kubectl -n steam-ft delete pod datanode-1
```

Dùng delete bình thường, không force-delete. Đây kiểm tra restart/leader election ngắn, chưa phải mất máy thật. Tạo lượt observer mới cho mỗi loại lỗi: xem/lưu log rồi xóa riêng Job `ft-watch` đã hoàn tất trước khi chạy lại. Ghi timestamp thao tác gây lỗi và thời điểm dịch vụ phục hồi.

**Lượt B — mất worker:** ưu tiên worker 1 chứa Kafka/DataNode. Kiểm tra pod map trước để chắc máy đó không chứa NameNode hoặc observer. Trên VM đó:

```bash
sudo systemctl stop k3s-agent
# Trên server quan sát node NotReady, log observer và trạng thái dịch vụ.
# Khôi phục trước khi kết thúc lượt thử:
sudo systemctl start k3s-agent
```

Dừng agent kiểm tra mất node Kubernetes; nếu yêu cầu mất máy/mạng thực sự, làm lượt riêng bằng tắt VM hoặc ngắt mạng VM rồi bật lại. Dừng agent có thể dừng container trên node tùy lifecycle và thời điểm; kiểm tra thực tế trạng thái tiến trình/pod thay vì chỉ dựa vào node NotReady. Mỗi lượt chỉ làm hỏng **một** máy, giữ hai Kafka voters còn lại và NameNode/observer hoạt động.

```bash
kubectl -n steam-ft wait --for=condition=complete job/ft-watch --timeout=600s
kubectl -n steam-ft logs job/ft-watch > /tmp/ft-watch-results.jsonl
bash scripts/k3s-fault-test.sh status
kubectl -n steam-ft exec namenode-0 -- hdfs fsck /steam/ft-probe -files -blocks -locations
```

Kết quả observer gồm ACK count, unique IDs đọc lại, `missing_acked`, số lỗi Kafka/HDFS và khoảng cách dài nhất giữa hai lần thành công. `INTEGRITY_OK` nghĩa là đã đọc lại đủ record được ACK và có lượt đọc HDFS thành công; **không có nghĩa không gián đoạn**. Đối chiếu gaps/errors với timestamp gây lỗi để kết luận availability. Observer không bảo đảm exactly-once; lần gửi timeout có thể vẫn được Kafka nhận và có thể trùng khi retry. File marker kiểm tra khả năng đọc bản sao đã có, không chứng minh ghi HDFS vẫn đủ replication khi mất một DataNode.

Sau phục hồi, xác nhận ISR trở lại ba, HDFS đủ hai bản sao và không missing block. Với local-path, PVC vẫn gắn máy ban đầu; khi máy mất, pod có thể Pending và không chuyển volume sang máy khác. Kafka/HDFS phục vụ bằng bản sao ứng dụng trên máy còn sống; không phải volume được Kubernetes tự di chuyển.

Không dùng script chaos cũ `tests/fault_tolerance/chaos_test_scenarios.sh` để kết luận cho môi trường này: script đó trỏ namespace khác và có thông báo PASS chưa kiểm tra dữ liệu.

## 4. Thêm máy thứ tư/thứ năm cho executor

Worker mới dùng cùng chuẩn VM, Tailscale và phiên bản k3s. Mỗi VM compute nên có 4 vCPU, 6–8 GiB RAM và khoảng 60 GiB SSD; mức thật phụ thuộc đo tải. Không thêm server/control plane cho giai đoạn này.

Trên VM thứ tư/thứ năm, thay IP bằng server Tailscale IP:

```bash
sudo bash infra/k3s/install-node.sh agent steam-worker-3 100.X.Y.Z
# Trên máy thứ năm:
sudo bash infra/k3s/install-node.sh agent steam-worker-4 100.X.Y.Z
```

Trên server:

```bash
bash scripts/k3s-fault-test.sh add-compute steam-worker-3
# Chỉ khi có máy thứ năm:
bash scripts/k3s-fault-test.sh add-compute steam-worker-4
kubectl get nodes -L steam.insights/compute,steam.insights/ft-storage
```

Worker mới chỉ nhận nhãn `steam.insights/compute=true`, không nhận nhãn ft-storage và không đổi vị trí volume/Kafka. Import cùng tar image trên các worker mới trước khi chạy benchmark.

## 5. Đo 1 → 2 → 4 executor thật

```bash
bash scripts/k3s-fault-test.sh benchmark 1 1000000
kubectl -n steam-ft get jobs,pods -o wide
# Đợi Job xong và lưu log, rồi chạy lần tiếp:
bash scripts/k3s-fault-test.sh benchmark 2 1000000
# Cần ít nhất hai node compute cho cấu hình VM nhỏ:
bash scripts/k3s-fault-test.sh benchmark 4 1000000
```

Script tạo Job có tên riêng cho mỗi lượt và từ chối chạy nếu còn driver/executor đang Running. Driver gọi native Spark-on-Kubernetes, tạo executor pods bằng ServiceAccount/RBAC trong steam-ft. Đây không phải `local[2]`. Driver giới hạn 2 GiB; mỗi executor heap 1 GiB + overhead 512 MiB, một core. Template phân bố executor cân bằng giữa các node compute theo hostname; không yêu cầu mỗi executor một máy, nên bốn executor có thể chạy trên hai/ba compute node.

Benchmark cố định số dòng và 16 partition, tắt dynamic allocation/AQE để so sánh dễ hơn, chạy groupBy/shuffle rồi ghi/đọc Parquet HDFS. Kiểm tra tổng số record và tổng ID để xác nhận kết quả. Log JSON tách thời gian chờ executor (`startup_seconds`) và thời gian workload (`workload_seconds`). Kết quả này đo workload tổng hợp, chưa phải hiệu năng B2 của đề tài; với một triệu dòng, overhead mạng/khởi động có thể khiến nhiều executor chậm hơn.

Chạy mỗi cấu hình ít nhất ba lượt, giữ cùng số dòng và trạng thái cụm, ghi số máy compute, executor, thời gian, RAM/CPU và độ trễ mạng. Khi muốn thử tăng dữ liệu, giữ số executor rồi đổi số dòng 1×/2×/5×. Không lấy log từ script benchmark cũ làm kết quả tự động vì job B2 hiện không có tham số `--scale` mà script cũ truyền.

```bash
kubectl -n steam-ft logs job/TEN_JOB > /tmp/TEN_JOB.log
kubectl -n steam-ft wait --for=condition=complete job/TEN_JOB --timeout=1800s
kubectl top nodes
kubectl -n steam-ft top pods
kubectl -n steam-ft get pods -l spark-role=executor -o wide
```

Tăng node khi có `Pending` do thiếu CPU/RAM, OOM đã xác định từ log, hoặc muốn tách tải phân tích khỏi lưu trữ. Thêm node không tự tăng executor của job đang chạy; chọn số executor ở lượt mới. Các Job giữ log sau khi hoàn tất; chủ động dọn Job đã lưu kết quả để tránh quota pod bị đầy. Không xóa namespace/PVC để dọn log.

Nguồn: [Kafka KRaft](https://kafka.apache.org/39/operations/kraft/), [Kafka broker settings](https://kafka.apache.org/39/configuration/broker-configs/), [HDFS setrep](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html#setrep), [Spark on Kubernetes 3.5.7](https://spark.apache.org/docs/3.5.7/running-on-kubernetes.html).
