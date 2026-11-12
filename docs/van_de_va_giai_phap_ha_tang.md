# Vấn đề tài nguyên phần cứng và hướng giải quyết

**Đề tài:** Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam
**Nhóm:** 5 thành viên, mỗi người một PC RAM 16 GB, thời gian 8 tuần
**Cập nhật:** 12/11/2026. Bản đầu nêu đề xuất; bản này ghi những gì nhóm đã áp dụng và đã đo. Quyết định chi tiết ở [docs/decisions.md](decisions.md).

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
| 1 | RAM hạn chế | Mỗi máy 16 GB; Windows và trình duyệt chiếm 6–8 GB, WSL thực tế có khoảng 8 GB |
| 2 | Các thành phần đều nặng | Kafka, Spark, Elasticsearch, HDFS đều chạy JVM, mỗi thành phần cần 1–3 GB |
| 3 | Dung lượng lưu trữ | Ước lượng ban đầu 20–30 GB thô, 70–110 GB sau khi nhân bản (replication ×3) |
| 4 | Không có cụm nhiều node thật | Không có server; các máy ở nhà khác nhau, mạng khác nhau |
| 5 | Tiến độ ngắn | 8 tuần; nếu cài hạ tầng quá lâu sẽ không còn thời gian cho phân tích và kiểm thử |
| 6 | Nguồn dữ liệu bị giới hạn tốc độ | API Steam khoảng 1 request/1,5 giây; không có API lịch sử CCU, nên CCU phải tự thu dần theo thời gian thực |

Khó khăn số 6 được bổ sung sau khi crawl thật ngày 12/11/2026.

### 1.3. Mâu thuẫn cần giải quyết

> Đề bài cần **nhiều node** để test phân tán, nhưng mỗi máy chỉ có **16 GB RAM**, và không được dùng Docker để thay Kubernetes.

---

## 2. Tham khảo: nhóm kỳ trước đã làm thế nào

Báo cáo "Hệ thống lưu trữ, xử lý, phân tích và dự đoán dữ liệu mạng xã hội" (Lambda Architecture) chạy toàn bộ trên **một cụm Minikube, 10 GB RAM, 4 CPU**.

| Vấn đề họ gặp | Cách xử lý |
|---|---|
| Dữ liệu lớn | Chỉ dùng 3,7 GB, replay từ file tĩnh vào Kafka với tốc độ 20 bản ghi/giây |
| Spark hết bộ nhớ | `shuffle.partitions` = 4, executor 2g, driver 1g, `persist(DISK_ONLY)` một lần cho nhiều view |
| Pod bị OOMKilled | `requests` thấp, `limits` gấp 1,5–2 lần, thêm `ResourceQuota` cho namespace |
| Model NLP nặng | Biến môi trường chuyển sang chế độ nhẹ (từ điển cảm xúc) khi thiếu RAM |
| Quá nhiều file nhỏ trên object storage | Flush khi đủ 500 bản ghi hoặc 60 giây |
| ClickHouse chậm trên đường phục vụ chính | Chuyển serving sang Elasticsearch + Redis |

**Điểm họ chưa làm được:** chỉ có một node, nên **không có kiểm thử scalability và fault-tolerance** thật. Đề năm nay yêu cầu điều này, nên nhóm mình cần làm khác.

---

## 3. Hướng giải quyết: đề xuất ban đầu và hiện trạng

### 3.1. Nguyên tắc (giữ nguyên)

1. **Thu nhỏ dữ liệu** để vừa phần cứng nhưng vẫn đủ chứng minh kiến trúc
2. **Chạy cụm nhiều node thật** để test được phân tán
3. **Tinh chỉnh tài nguyên từng pod** theo kinh nghiệm nhóm kỳ trước
4. **Cắt bớt thành phần không cần thiết**

### 3.2. Dữ liệu

| Hạng mục | Thiết kế gốc | Đề xuất ban đầu | Đã áp dụng (12/11/2026) |
|---|---|---|---|
| Nguồn | Steam + dataset lịch sử | Thêm vài GB dataset Kaggle, replay qua Kafka | **Chỉ API chính thức của Steam.** Bỏ Kaggle, Hugging Face, SteamSpy vì trộn nguồn làm sai các chỉ số theo thời gian |
| Số game | 1.000–2.000 | 200–300, top theo người chơi/review | **100 game cho demo**: 20 top CCU + 80 lấy mẫu phân tầng theo số review. Sau đó mở rộng khoảng 1.000 game, dùng lại dữ liệu đã thu |
| Review | Hàng triệu | — | 92.019 review, 0 trùng; crawl tăng dần để lần thu sau không lấy lại |
| Lịch sử CCU | — | — | Tự thu dần; cần chạy liên tục ≥ 14 ngày |
| Replication HDFS | 3 | 2 | 2 (trong `k8s/fault-test`) |
| Dung lượng | 70–110 GB | 10–20 GB | Hiện tại raw 18,3 MiB, lake 166 MiB; tăng dần theo vòng thu |

Bộ demo nhỏ có hai lợi ích: chạy toàn trình được trên một máy, và nguồn gốc dữ liệu rõ ràng. Quy mô "big data" sẽ đến từ việc mở rộng danh sách game và thu liên tục, không đến từ dataset bên ngoài.

### 3.3. Môi trường chạy

| Mục đích | Đề xuất ban đầu | Đã áp dụng |
|---|---|---|
| Phát triển, demo | Local mode trên mẫu nhỏ | **Một máy (WSL2, khoảng 8 GB):** Spark local; Kafka, ES, Kibana chạy bằng Docker Compose, bật theo nhóm. Đã chạy toàn trình ngày 12/11/2026 |
| Cụm phân tán | 5 VM trên máy cá nhân, nối Tailscale | **4 VM Google Cloud `e2-standard-2`** (1 server + 3 agent), k3s, Tailscale, chỉ bật theo buổi. Ghép máy cá nhân là phương án dự phòng |
| Dự phòng | Oracle Cloud Always Free | Azure for Students → máy cá nhân qua Tailscale → Oracle Cloud |

Lý do bỏ phương án 5 máy cá nhân: mạng nhà không ổn định, máy thành viên tắt bất kỳ lúc nào, khó lặp lại kiểm thử. VM cloud bật theo buổi tốn ít tiền và cho kết quả đo ổn định hơn.

### 3.4. Tài nguyên đo được trên một máy (12/11/2026)

| Thành phần | Cấu hình | RAM thực tế |
|---|---|---|
| Kafka (KRaft, 1 broker) | heap 512 MB, giới hạn container 1 GB | khoảng 280 MB |
| Elasticsearch (1 node) | heap 512 MB, giới hạn 1 GB | khoảng 815 MB |
| Kibana | giới hạn 1 GB | khoảng 490 MB |
| Spark batch B2–B8 | `local[4]`, driver 2–3 GB | 2–3 GB; khoảng 12 phút cho 92 nghìn review, B5 chậm nhất |
| Spark streaming S1–S3 | 3 job, `local[2]`, driver 1 GB mỗi job | khoảng 1 GB mỗi job |

Bật đồng thời Kafka, ES, Kibana và 3 job streaming thì máy 8 GB gần hết RAM. Cách làm: dừng streaming khi chạy batch (`scripts/demo.sh stream-stop`). Không cần bật HDFS để chạy local.

### 3.5. Cấu hình thu gọn trên cụm

| Thành phần | Cấu hình gọn | RAM ước tính | Trạng thái |
|---|---|---|---|
| Kafka (KRaft) | 3 broker, heap 512 MB, RF=3, min ISR=2 | khoảng 3 GB | Có manifest |
| HDFS | 1 NameNode + 2 DataNode, replication 2 | khoảng 3 GB | Có manifest |
| Spark | Driver + executor theo job, 1–1,5 GB/executor | khoảng 4 GB | Có manifest; image cần build lại với code 12/11 |
| Elasticsearch | 1 node, 0 replica (đề xuất ban đầu: 2 node) | khoảng 2 GB | Có manifest; chưa đủ cho FT4/SC4 |
| Kibana | 1 pod | khoảng 1 GB | Có manifest |
| Crawler | CronJob, chia worker theo appid | khoảng 0,5 GB | Cần thêm CronJob `store`/`histogram` |
| k3s + hệ thống | 4 node | khoảng 2 GB | Chưa dựng |
| **Tổng** | | **khoảng 15–16 GB** trên 32 GB của cụm | |

Giữ **3 Kafka broker** để test fault-tolerance có ý nghĩa.

### 3.6. Cắt bớt thành phần

| Bỏ / thay | Lý do | Trạng thái |
|---|---|---|
| Hive Metastore + PostgreSQL | Spark SQL đọc trực tiếp Parquet (`createOrReplaceTempView`) vẫn đủ làm lớp truy vấn | Đã bỏ trên cụm thu gọn; job tự đăng ký bảng nếu có Metastore |
| Prometheus + Grafana | Dùng `kubectl top` và Kafka consumer lag cho số liệu kiểm thử | Chưa triển khai; D5 chỉ có khi bật trên cụm |
| SteamSpy | Số liệu ước lượng của bên thứ ba; tag lấy được từ Steam | Đã bỏ |
| Không chạy batch và streaming cùng lúc | `scripts/demo.sh stream-stop` khi chạy batch; trên cụm dùng `kubectl scale --replicas=0` | Đang áp dụng |

### 3.7. Cấu hình Kubernetes học từ nhóm kỳ trước

**Đặt `requests` và `limits` cho mọi pod.** Với dịch vụ JVM, `limits` nên gấp 1,5–2 lần `requests`:

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

### 3.8. Mẹo xử lý Spark

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")

df = spark.read.parquet(path).repartition(8)
df = df.persist(StorageLevel.DISK_ONLY)   # đọc một lần, dùng cho nhiều view
# ... tính các batch view ...
df.unpersist()
```

- Viết và debug job ở **local mode** trước, rồi mới submit lên cụm.
- Quy tắc: số shuffle partitions khoảng 2–4 lần số core của executor.
- Bài học ngày 12/11: với dữ liệu nhỏ, thời gian khởi động Spark chiếm phần lớn, nên tăng core không làm job nhanh hơn. Đo SC1 cần dữ liệu đủ lớn.

### 3.9. File nhỏ (chưa xử lý)

Đề xuất ban đầu là flush khi đủ 500 bản ghi hoặc sau 60 giây. Hiện tại chưa làm được như vậy:

- **Raw:** crawler local flush sau mỗi game và mỗi nguồn, để lưu state ngay sau khi dữ liệu đã nằm trên đĩa. Kết quả là 1.020 file review cho 92 nghìn review, và CCU chỉ 1 bản ghi một file.
- **Clean:** `clean/reviews` có 10.737 file Parquet do phân vùng theo ngày tạo review (2013–2026).

Ở quy mô 100 game thì chưa ảnh hưởng. Trước khi mở rộng và đưa lên HDFS cần:
- Gộp file raw theo ngày.
- Cho B2 `repartition` theo `dt` trước khi ghi.

### 3.10. Chế độ nhẹ cho phân tích cảm xúc

Dùng từ điển cảm xúc (VADER) và từ điển khía cạnh, chỉ phân tích review tiếng Anh. Đề bài nói rõ môn học **không tập trung vào mô hình học máy**, nên chế độ nhẹ là chấp nhận được.

---

## 4. Đáp ứng yêu cầu của đề bài

| Yêu cầu của cô | Cách đáp ứng | Hiện trạng |
|---|---|---|
| K8s hoặc cloud, không Docker | Cụm **k3s 4 VM** trên Google Cloud; Docker Compose chỉ dùng để phát triển và demo trên một máy | Manifest sẵn sàng; chưa chạy cụm |
| Mô phỏng môi trường phân tán | 4 VM, pod rải các node bằng nhãn node/`podAntiAffinity` | Chưa chạy |
| Test scalability | Tăng executor 1 → 2 → 4 → 6, tăng partition Kafka, tăng dữ liệu 1× → 10×, ES 2 → 3 node | Script chạy thử được trên một máy; số đo chính thức cần cụm |
| Test fault-tolerance | Xoá pod Kafka/DataNode/executor/driver, tắt hẳn một VM, API lỗi 429 | FT6 đạt; FT1–FT5 cần cụm |
| Dashboard hữu ích | Kibana D1 tổng quan, D2 chi tiết game, D3 realtime, D4 cảnh báo và xu hướng | Đã chạy trên dữ liệu thật |

---

## 5. Rủi ro và phương án dự phòng

| Rủi ro | Phương án |
|---|---|
| Hết credit / quên tắt VM cloud | Bật theo buổi, đặt Budget alert; dự phòng Azure for Students hoặc máy cá nhân |
| Máy chạy vòng thu CCU bị tắt | Mất dữ liệu khoảng đó (không lấy lại được); để một máy bật 24/7, chuyển sang VM hoặc CronJob khi có cụm |
| Steam giới hạn tốc độ khi mở rộng danh sách | Crawl tăng dần, chạy nhiều lượt; chia worker trên cụm |
| Hết RAM khi chạy đủ thành phần | Bật dịch vụ theo nhóm; dừng streaming khi chạy batch; trên cụm dùng `kubectl scale --replicas=0` |
| Cài HDFS trên K8s quá khó | Dùng manifest có sẵn; nếu bế tắc, thay bằng **MinIO** (cần hỏi cô trước) |
| Chậm tiến độ cài hạ tầng | Pipeline đã chạy toàn trình local; buổi trên cloud chỉ để triển khai và đo |

---

## 6. Câu hỏi cần xác nhận với cô

1. Có thể dùng **MinIO (S3)** thay **HDFS** để tiết kiệm tài nguyên không?
2. Cụm k3s 4 VM cloud nối bằng Tailscale có được chấp nhận là "mô phỏng môi trường phân tán" không?
3. "No docker" có cấm Docker Compose dùng để phát triển và demo trên một máy không, khi phần nghiệm thu chạy trên k3s (containerd)?
4. Bộ demo 100 game / 92 nghìn review và mục tiêu mở rộng khoảng 1.000 game, chỉ lấy từ API Steam, có đủ để chứng minh "big data" không?

---

## 7. Kết luận

Hướng nhóm đã chọn và đang thực hiện:

> **Dữ liệu chỉ từ Steam, bắt đầu 100 game rồi mở rộng bằng thu liên tục + phát triển và demo toàn trình trên một máy + cụm k3s 4 VM cloud bật theo buổi để kiểm thử phân tán thật bằng cách tắt pod/node.**

Ngày 12/11/2026, phần trên một máy đã chạy được toàn bộ luồng Lambda với dữ liệu thật. Phần còn lại là chạy trên cụm và đo scalability, fault-tolerance — điều nhóm kỳ trước (một Minikube đơn node) chưa làm được.
