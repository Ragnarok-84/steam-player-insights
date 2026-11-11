# Vấn đề tài nguyên phần cứng và hướng giải quyết

**Đề tài:** Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam
**Nhóm:** 5 thành viên, mỗi người một PC RAM 16 GB, thời gian 8 tuần

---

## 1. Vấn đề

### 1.1. Yêu cầu của đề bài

Theo slide của cô, hệ thống cần:

- Có đủ các thành phần: thu thập, lưu trữ, xử lý, truy vấn, trực quan hoá (kiến trúc Lambda)
- Chạy trên **Kubernetes hoặc cloud, không dùng Docker**
- **Mô phỏng môi trường phân tán**
- **Kiểm thử scalability và fault-tolerance**

### 1.2. Khó khăn thực tế

| # | Khó khăn | Chi tiết |
|---|---|---|
| 1 | RAM hạn chế | Mỗi máy 16 GB, Windows và trình duyệt đã chiếm 6-8 GB, chỉ còn khoảng 8 GB cho hệ thống |
| 2 | Các thành phần đều nặng | Kafka, Spark, Elasticsearch, HDFS đều chạy JVM, mỗi thành phần cần 1-3 GB |
| 3 | Dung lượng lưu trữ | Ước lượng ban đầu 20-30 GB thô, 70-110 GB sau khi nhân bản (replication ×3) |
| 4 | Không có cụm nhiều node thật | Không có server, các máy ở nhà khác nhau, mạng khác nhau |
| 5 | Tiến độ ngắn | 8 tuần, nếu cài đặt hạ tầng quá lâu sẽ không còn thời gian cho phân tích và kiểm thử |

### 1.3. Mâu thuẫn cần giải quyết

> Đề bài cần **nhiều node** để test phân tán, nhưng mỗi máy chỉ có **16 GB RAM**, và không được dùng Docker để thay Kubernetes.

---

## 2. Tham khảo: nhóm kỳ trước đã làm thế nào

Báo cáo "Hệ thống lưu trữ, xử lý, phân tích và dự đoán dữ liệu mạng xã hội" (Lambda Architecture) chạy toàn bộ trên **một cụm Minikube, 10 GB RAM, 4 CPU**.

**Những điểm đáng học:**

| Vấn đề họ gặp | Cách xử lý |
|---|---|
| Dữ liệu lớn | Chỉ dùng 3,7 GB, replay từ file tĩnh vào Kafka với tốc độ 20 bản ghi/giây |
| Spark hết bộ nhớ | `shuffle.partitions` = 4, executor 2g, driver 1g, `persist(DISK_ONLY)` một lần cho nhiều view |
| Pod bị OOMKilled | `requests` thấp, `limits` gấp 1,5-2 lần, thêm `ResourceQuota` cho namespace |
| Model NLP nặng | Biến môi trường chuyển sang chế độ nhẹ (từ điển cảm xúc) khi thiếu RAM |
| Quá nhiều file nhỏ trên object storage | Flush khi đủ 500 bản ghi hoặc 60 giây |
| ClickHouse chậm trên đường phục vụ chính | Chuyển serving sang Elasticsearch + Redis |

**Điểm họ chưa làm được:** chỉ có một node, nên **không có kiểm thử scalability và fault-tolerance** thật. Đây là điều đề năm nay yêu cầu, nên nhóm mình cần làm khác.

---

## 3. Hướng giải quyết đề xuất

### 3.1. Nguyên tắc

1. **Thu nhỏ dữ liệu** để vừa phần cứng nhưng vẫn đủ chứng minh kiến trúc
2. **Ghép nhiều máy thành cụm nhiều node thật** để test được phân tán
3. **Tinh chỉnh tài nguyên từng pod** theo kinh nghiệm nhóm kỳ trước
4. **Cắt bớt thành phần không cần thiết**

### 3.2. Thu nhỏ quy mô dữ liệu

| Hạng mục | Ban đầu | Điều chỉnh |
|---|---|---|
| Số game theo dõi | 1.000-2.000 | **200-300** (top theo số người chơi/số review) |
| Replication HDFS | 3 | **2** |
| Dữ liệu thô giữ lại | Lâu dài | Nén gzip, chuyển sang Parquet rồi xoá bớt raw |
| Dữ liệu lịch sử | Vài chục GB | **Vài GB** từ dataset Kaggle, replay qua Kafka |
| **Tổng dung lượng** | 70-110 GB | **khoảng 10-20 GB** |

### 3.3. Dựng cụm nhiều node bằng máy cá nhân

- Mỗi thành viên chạy **1 VM Linux** (Multipass/VirtualBox, hoặc WSL2 nếu dùng Windows)
- Cấp cho mỗi VM: **8 GB RAM, 4 vCPU, 40-60 GB đĩa**
- Nối các VM bằng **Tailscale** (VPN mesh, miễn phí)
- Cài **k3s**: 1 node server + 4 node worker, tổng khoảng 40 GB RAM
- Đặt node server ở máy ổn định nhất, ít tắt máy nhất

Nếu dùng WSL2, giới hạn RAM trong `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=8GB
processors=4
```

**Phương án dự phòng:** dùng **Oracle Cloud Always Free** (4 CPU ARM, 24 GB RAM) làm một node. Các image Kafka, Spark, Elasticsearch đều có bản ARM64.

### 3.4. Thu gọn cấu hình từng thành phần

| Thành phần | Cấu hình gọn | RAM ước tính |
|---|---|---|
| Kafka (KRaft) | 3 broker, heap 512 MB, RF=3, min ISR=2 | ~3 GB |
| HDFS | 1 NameNode + 2 DataNode, replication=2 | ~3 GB |
| Spark | 1 driver + 2 executor (1-1,5 GB/executor), chạy theo job | ~4 GB |
| Elasticsearch | 2 node, heap 1 GB | ~4 GB |
| Kibana | 1 pod | ~1 GB |
| Crawler | 2 pod nhỏ | ~0,5 GB |
| k3s + hệ thống | 5 node | ~2,5 GB |
| **Tổng** | | **≈ 18 GB** |

Tổng này nằm gọn trong 40 GB của cụm, còn dư để tăng executor khi test scalability.

### 3.5. Cắt bớt thành phần

| Bỏ / thay | Lý do | Tiết kiệm |
|---|---|---|
| Bỏ Hive Metastore + PostgreSQL | Dùng Spark SQL đọc trực tiếp Parquet trên HDFS (`createOrReplaceTempView`) vẫn đủ làm lớp truy vấn | ~1,5 GB |
| Prometheus + Grafana là tuỳ chọn | Dùng `kubectl top` và Kafka consumer lag để lấy số liệu kiểm thử | ~1-2 GB |
| Không chạy batch và streaming cùng lúc khi phát triển | `kubectl scale --replicas=0` phần chưa dùng | tuỳ lúc |

Giữ nguyên **3 Kafka broker** để test fault-tolerance có ý nghĩa.

### 3.6. Cấu hình Kubernetes học từ nhóm kỳ trước

**Đặt `requests` và `limits` cho mọi pod.** `limits` nên gấp 1,5-2 lần `requests` với các dịch vụ JVM:

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 1000m
    memory: 2Gi
```

**Thêm readiness probe cho mọi container**, để Kubernetes không gửi traffic vào pod chưa sẵn sàng.

**Giới hạn tổng tài nguyên namespace** bằng `ResourceQuota`.

### 3.7. Mẹo xử lý Spark

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.parquet(path).repartition(8)
df = df.persist(StorageLevel.DISK_ONLY)   # đọc một lần, dùng cho nhiều view
# ... tính các batch view ...
df.unpersist()
```

- Viết và debug job ở **local mode** trên mẫu nhỏ trước, rồi mới submit lên cụm
- Quy tắc: số shuffle partitions ≈ 2-4 lần số core của executor

### 3.8. Flush dữ liệu xuống HDFS theo hai điều kiện

Tránh sinh quá nhiều file nhỏ: flush khi đủ **500 bản ghi** hoặc sau **60 giây**, điều kiện nào đến trước.

```python
FLUSH_SIZE = 500
FLUSH_INTERVAL = 60

if total_buffered >= FLUSH_SIZE or \
   (elapsed >= FLUSH_INTERVAL and total_buffered > 0):
    flush_all(buffer)
    buffer.clear()
    last_flush = time.time()
```

### 3.9. Nếu thiếu RAM: chế độ nhẹ cho phân tích cảm xúc

Dùng từ điển cảm xúc (lexicon) làm mặc định, chỉ bật mô hình transformer khi đủ RAM, điều khiển bằng biến môi trường:

```python
LIGHTWEIGHT = os.getenv("SPEED_LIGHTWEIGHT", "true").lower() in ("1", "true", "yes")
```

Đề bài nói rõ môn học **không tập trung vào mô hình học máy**, nên chế độ nhẹ là chấp nhận được.

---

## 4. Đáp ứng yêu cầu của đề bài

| Yêu cầu của cô | Cách đáp ứng |
|---|---|
| K8s hoặc cloud, không Docker | Cụm **k3s nhiều node**, triển khai bằng Helm/Operator |
| Mô phỏng môi trường phân tán | 5 VM ở 5 máy khác nhau nối bằng Tailscale, pod được rải các node bằng `podAntiAffinity` |
| Test scalability | Tăng executor Spark 1 → 2 → 4, tăng partition Kafka, tăng dữ liệu 1× → 2× → 5×; đo thời gian và thông lượng |
| Test fault-tolerance | Xoá pod Kafka/DataNode/executor, tắt hẳn một node worker; ghi thời gian phục hồi và dữ liệu có bị mất không |
| Dashboard hữu ích | Kibana: tổng quan, chi tiết game, realtime, cảnh báo |

---

## 5. Rủi ro và phương án dự phòng

| Rủi ro | Phương án |
|---|---|
| Mạng nhà không ổn định, Tailscale chậm | Đặt control plane ở máy mạng tốt nhất; dự phòng dùng Oracle Cloud làm node chính |
| Một máy tắt đột ngột | Đây chính là kịch bản fault-tolerance; ghi lại làm kết quả kiểm thử |
| Cài HDFS/Hive trên K8s quá khó | Dùng Helm chart có sẵn; nếu bế tắc, thay HDFS bằng **MinIO** (nhóm kỳ trước dùng MinIO), cần hỏi cô trước |
| Hết RAM khi chạy đủ thành phần | Tắt Kibana/Elasticsearch khi chạy test Spark; dùng `kubectl scale --replicas=0` |
| Chậm tiến độ cài hạ tầng | Người phụ trách hạ tầng bắt đầu ngay tuần 1; các thành viên khác phát triển trên dữ liệu mẫu ở local mode |

---

## 6. Câu hỏi cần xác nhận với cô

1. Có thể dùng **MinIO (S3)** thay **HDFS** để tiết kiệm tài nguyên không?
2. Cụm k3s ghép từ các VM trên máy cá nhân qua VPN có được chấp nhận là "mô phỏng môi trường phân tán" không?
3. "No docker" có nghĩa là cấm cả container runtime trong K8s (containerd là mặc định của k3s), hay chỉ cấm dùng Docker/Docker Compose thay cho Kubernetes?
4. Quy mô 200-300 game và vài GB dữ liệu có đủ để chứng minh "big data" không?

---

## 7. Kết luận

Nhóm không cần cloud đắt tiền hay máy chủ riêng. Hướng phù hợp nhất là:

> **Thu nhỏ dữ liệu (200-300 game, 10-20 GB) + ghép 5 VM (mỗi VM 8 GB) thành cụm k3s qua Tailscale + cấu hình gọn từng thành phần + kiểm thử phân tán thật bằng cách tắt pod/node.**

Cách này vừa nằm trong phần cứng của nhóm, vừa đáp ứng đủ yêu cầu phân tán, scalability và fault-tolerance mà đề bài đặt ra, điều mà nhóm kỳ trước (chạy một Minikube đơn node) chưa làm được.
