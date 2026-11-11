# Cụm k3s ba máy và demo một instance

Bộ cấu hình để nhóm thống nhất trước khi cài: ba máy cá nhân, mỗi máy một VM Ubuntu 24.04 LTS, một server và hai agent. Các dịch vụ chạy trên Kubernetes/containerd, không dùng Docker Compose. Cấu hình đã kiểm tra tĩnh; chưa được chạy trên ba VM thực tế.

Sau mốc demo một instance, dùng [hướng dẫn kiểm thử chịu lỗi và mở rộng](FAULT_TEST.md): môi trường riêng Kafka ba broker/HDFS hai DataNode, thêm worker compute thứ tư/thứ năm và benchmark Spark 1/2/4 executor.

## 1. Thống nhất ba máy

| VM | Tài nguyên khởi đầu | Nhãn node | Dịch vụ |
| --- | --- | --- | --- |
| `steam-server` | 4 vCPU, 6–8 GB RAM, 60 GB SSD | `storage` | k3s server, NameNode, Elasticsearch |
| `steam-worker-1` | 4 vCPU, 6–8 GB RAM, 60 GB SSD | `ingest` | Kafka, DataNode |
| `steam-worker-2` | 4 vCPU, 8 GB RAM, 60 GB SSD | `compute` | Kibana, một Spark Job mỗi lần |

Trên máy Windows 16 GB RAM, dành phần còn lại cho Windows và ứng dụng. Đây là mức cấp cho VM; đo bằng `free -h`, `kubectl top nodes` và `kubectl top pods` rồi điều chỉnh. Ba VM phải chạy trên ba máy vật lý khác nhau để thử phân tán giữa máy thật. Giữ máy server và các VM bật trong buổi tích hợp, tắt sleep tự động trong buổi chạy.

Tạo VM bằng Hyper-V/VirtualBox hoặc công cụ VM nhóm đã có. Cài Ubuntu 24.04 LTS, bật SSH, dùng tên VM/hostname như bảng và clone repo trong VM. Không cần cài Docker. Ưu tiên VM cho cụm chung; WSL2 vẫn dùng để phát triển local.

Mỗi VM cần Internet để tải image. Đĩa chứa `/var/lib/rancher/k3s` phải có chỗ cho image và volume. PVC demo xin 10 GiB mỗi dịch vụ có dữ liệu; local-path không tự áp hạn mức dung lượng đĩa, nên vẫn phải theo dõi `df -h`.

## 2. Kết nối Tailscale trước khi cài k3s

Trên từng VM:

```bash
sudo apt update
sudo apt install -y curl ca-certificates git
curl -fsSL https://tailscale.com/install.sh -o /tmp/install-tailscale.sh
sudo sh /tmp/install-tailscale.sh
sudo tailscale up
tailscale ip -4
```

Nhóm thống nhất một tailnet và mời các thành viên vào đó; đăng nhập rồi cấp quyền để các VM liên lạc được với nhau. Ghi vào bảng bàn giao: tên node, chủ máy và IP Tailscale. Không commit token hoặc auth key.

Từ mỗi VM, thử `tailscale ping IP_VM_KHAC`. Kiểm tra có đường trực tiếp hay qua relay; ghi nhận độ trễ trước khi benchmark. Nếu cấu hình ACL/firewall riêng, cho phép giữa ba VM qua Tailscale: TCP 6443 (API/server), TCP 10250 (kubelet/metrics), UDP 8472 (Flannel VXLAN). Không mở các cổng này ra Internet/router. DNS và traffic dịch vụ/pod đi trong mạng overlay; chính sách firewall forwarding cũng phải cho phép mạng pod `10.42.0.0/16` và service `10.43.0.0/16`.

Thiết kế này dùng Tailscale đã đăng nhập và Flannel VXLAN trên `tailscale0`: `node-ip` là IP Tailscale, `flannel-iface` là `tailscale0`. Không dùng backend `host-gw` vì các VM khác mạng LAN. Nếu CIDR mặc định trùng mạng đang có, chốt CIDR khác trước khi tạo cụm. Đây là cấu hình kết hợp cần kiểm tra cross-node thực tế, không phải chế độ `--vpn-auth` thử nghiệm của k3s.

## 3. Cài một server và hai agent

Script khóa k3s `v1.34.12+k3s1` trên cả ba VM, dùng SQLite cho một server, giữ local-path và metrics-server mặc định, tắt Traefik/ServiceLB để giảm tải. Server vẫn được chạy workload. Script cần sudo và sẽ từ chối ghi đè cấu hình/cài đặt k3s đã có.

Trên VM server, tại gốc repo:

```bash
sudo bash infra/k3s/install-node.sh server steam-server
sudo k3s kubectl get nodes -o wide
```

Lấy token từ `/var/lib/rancher/k3s/server/node-token` trên server và gửi riêng cho hai người quản lý worker. Token cấp quyền gia nhập cụm; không đưa vào chat nhóm công khai, Git hoặc ảnh chụp màn hình.

Trên worker 1 và worker 2, thay `100.X.Y.Z` bằng IP Tailscale của server; script hỏi token bằng input ẩn:

```bash
# Chạy trên worker 1:
sudo bash infra/k3s/install-node.sh agent steam-worker-1 100.X.Y.Z

# Chạy trên worker 2:
sudo bash infra/k3s/install-node.sh agent steam-worker-2 100.X.Y.Z
```

Trên server, tạo bản kubeconfig riêng cho người quản lý cụm:

```bash
mkdir -p "$HOME/.kube"
sudo install -m 600 -o "$(id -u)" -g "$(id -g)" \
  /etc/rancher/k3s/k3s.yaml "$HOME/.kube/steam-demo.yaml"
export KUBECONFIG="$HOME/.kube/steam-demo.yaml"
kubectl get nodes -o wide
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl get pods -n kube-system
```

Phải có đúng ba node mong đợi ở trạng thái Ready, InternalIP là IP Tailscale, CoreDNS/local-path/metrics-server hoạt động. Bản kubeconfig trên dùng API localhost nên dùng ngay trên VM server; chưa bàn giao kubeconfig admin cho cả nhóm. Kubeconfig và token chứa quyền truy cập cụm, giữ ngoài repo.

## 4. Triển khai mỗi dịch vụ một instance

Trên server:

```bash
bash scripts/k3s-demo.sh label-nodes steam-server steam-worker-1 steam-worker-2
bash scripts/k3s-demo.sh render > /tmp/steam-demo-rendered.yaml
kubectl apply --dry-run=server -k k8s/demo
bash scripts/k3s-demo.sh deploy
bash scripts/k3s-demo.sh status
```

Manifest ở `k8s/demo/` dùng StatefulSet/Deployment và Service Kubernetes trực tiếp; chưa cần Helm/Operator cho mốc đầu này. Namespace riêng `steam-demo` có quota, mỗi pod có requests/limits và startup/readiness probe. Nhãn node chia tải đúng ba máy; không đặt anti-affinity bắt buộc ba replica như cấu hình cũ.

| Dịch vụ | Requests RAM | Limits RAM | Lưu trữ |
| --- | --- | --- | --- |
| NameNode | 512 MiB | 1 GiB | PVC 10 GiB |
| DataNode | 512 MiB | 1 GiB | PVC 10 GiB |
| Kafka | 512 MiB | 1 GiB | PVC 10 GiB |
| Elasticsearch | 1 GiB | 1,5 GiB | PVC 10 GiB |
| Kibana | 512 MiB | 1 GiB | Không có PVC riêng |
| Spark Job | 3 GiB | 4 GiB | Đầu ra ghi HDFS |

Kafka dùng KRaft, một broker kiêm controller, RF=1/min ISR=1. HDFS replication=1. Elasticsearch một node, mapping demo một primary shard và 0 replica. HDFS/ES tắt xác thực cho cụm thử riêng; các Service chỉ dùng ClusterIP/headless, không public NodePort/Ingress.

`deploy` đợi từng dịch vụ sẵn sàng, tạo thư mục HDFS và bốn topic Kafka. Readiness chỉ là bước đầu; chạy bài smoke ở dưới để xác nhận cross-node đọc/ghi. Bộ này chưa triển khai Hive, monitoring stack, crawler định kỳ hoặc streaming dài hạn.

## 5. Image Spark và bài kiểm tra luồng dữ liệu

Build image OCI bằng Podman trên VM compute. Image cài Python 3.11.16, Java 17 và `requirements.txt` đã khóa của nhóm, không dùng Docker daemon. Image gồm cả thư viện notebook/test để khớp môi trường nguồn nên build lần đầu có thể lâu; raw không được đóng vào image.

```bash
sudo apt install -y podman
podman build -t localhost/steam-spark-demo:k3s-20261026 \
  --ignorefile infra/k3s/containerignore \
  -f infra/k3s/Dockerfile.spark-demo .
podman save --format docker-archive \
  -o /tmp/steam-spark-demo.tar localhost/steam-spark-demo:k3s-20261026
sudo k3s ctr images import /tmp/steam-spark-demo.tar
sudo k3s ctr images list
```

Tên `Dockerfile` và định dạng `docker-archive` là định dạng build/image, không phải triển khai bằng Docker. k3s chạy containerd. Nếu quy định môn học yêu cầu cách build khác, dùng công cụ OCI tương đương. Job dùng `imagePullPolicy: Never` và nhãn `compute`, nên phải import đúng tag trên VM compute trước khi chạy; khi chuyển nhãn compute sang máy khác, import image lên máy đó.

Trên server:

```bash
bash scripts/k3s-demo.sh smoke
```

Job tạo topic tạm, gửi/nhận một bản ghi qua Kafka trên worker 1, dùng Spark trên worker 2 ghi/đọc Parquet qua NameNode server và DataNode worker 1, rồi ghi/đọc document ES trên server. Đồng thời tạo các index demo theo mapping của repo. Mỗi bước phải in `PASS`; Job phải `Complete`. Topic tạm được xóa sau bài thử, Parquet/document đánh dấu lượt chạy được giữ để đối chiếu.

Đây là bài kiểm tra tích hợp hạ tầng với dữ liệu mẫu, không chứng minh các job batch/streaming của đề tài đã hoàn thiện. Spark đang chạy `local[2]` trong một pod; các dịch vụ đã phân bố ba máy, nhưng chưa phải Spark driver/executor phân tán. Bước scalability sau cần Spark Operator hoặc cấu hình Spark cluster phù hợp.

## 6. Chạy B2 trên raw từ Drive

Tải/giải nén dữ liệu vào `data/raw/` trong repo trên VM server. Không cần copy raw lên cả ba máy; upload một lần vào HDFS:

```bash
bash scripts/k3s-demo.sh upload-raw
bash scripts/k3s-demo.sh b2
kubectl -n steam-demo exec namenode-0 -- hdfs dfs -ls /steam/clean /steam/clean/reviews
```

B2 nhận thư mục gốc raw (`/steam/raw`, chứa `steam_reviews_raw/`, `steam_players_ccu/`, `steam_games_meta/`) và thư mục gốc clean (`/steam/clean`), rồi ghi `clean/reviews`, `clean/ccu`, `clean/games`, `clean/games_history`. Job tính lại toàn bộ từ raw và ghi đè, nên chạy lại không sinh trùng. Script từ chối upload nếu thư mục raw đích đã có. `kubectl cp` tạm chép raw vào `/tmp` container NameNode trước khi đưa vào HDFS; kiểm tra dung lượng VM server và không coi bản tạm này là bản sao lưu. Bản gốc trên Drive vẫn giữ nguyên. Raw nhập HDFS không cần Kafka/B1.

Job chạy một lần và giữ log. Nếu cần chạy lại, xem log trước rồi chủ động xóa đúng Job (`kubectl -n steam-demo delete job demo-b2`) trước khi chạy lại:

```bash
kubectl -n steam-demo logs job/demo-b2
kubectl -n steam-demo describe job/demo-b2
```

Không xóa PVC hoặc format NameNode để chữa lỗi. Với local-path, volume nằm trên node đã cấp; node đó mất kết nối thì pod có thể Pending. Một instance không có HA, và một server là điểm lỗi đơn. Mốc 1–2 chỉ kiểm tra triển khai và luồng dữ liệu; tăng replica và kiểm thử chịu lỗi là bước riêng sau đó.

## 7. Truy cập giao diện và chẩn đoán

Trên VM server, giữ terminal port-forward:

```bash
bash scripts/k3s-demo.sh kibana
# Terminal khác nếu cần ES:
bash scripts/k3s-demo.sh es
```

Trên máy cá nhân ngoài VM, dùng SSH tunnel tới VM server. Nếu máy host cũng đã vào tailnet, có thể dùng IP Tailscale; nếu chưa, dùng địa chỉ VM mà host truy cập được:

```bash
ssh -L 5601:127.0.0.1:5601 USER@SERVER_IP
```

Mở <http://localhost:5601> rồi tạo data view cho các index thực sự có dữ liệu. Smoke chỉ tạo một document kiểm tra, B2 chỉ ghi Parquet; chưa có dashboard phân tích hoặc dữ liệu review trong ES tự động.

```bash
kubectl top nodes
kubectl -n steam-demo top pods
kubectl -n steam-demo describe pod POD_NAME
kubectl -n steam-demo logs POD_NAME
kubectl -n steam-demo get events --sort-by=.lastTimestamp
kubectl -n steam-demo exec namenode-0 -- hdfs dfsadmin -report
kubectl -n steam-demo exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe
```

`Pending`: kiểm tra nhãn, RAM allocatable, PVC và quota. `ImagePullBackOff`: kiểm tra Internet/tag image; `ErrImageNeverPull`: import image Spark trên compute. DNS/cross-node timeout: kiểm tra Tailscale, UDP 8472, firewall forwarding và CoreDNS. `OOMKilled`: đo lại RAM và tăng limits/heap tương ứng. Script triển khai dừng khi readiness thất bại; không bỏ qua lỗi để báo thành công.

Nguồn: [K3s network options](https://docs.k3s.io/networking/basic-network-options), [K3s requirements](https://docs.k3s.io/installation/requirements), [K3s 1.34 releases](https://docs.k3s.io/release-notes/v1.34.X), [distributed networking considerations](https://docs.k3s.io/networking/distributed-multicloud).
