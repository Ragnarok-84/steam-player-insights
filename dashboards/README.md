# Thiết Kế Dashboard - Steam Player Insights

Hệ thống cung cấp 5 bộ dashboard chính theo yêu cầu đề bài.

**Cài đặt:**
- Kibana D1–D4: chạy `python pipeline.py kibana-setup` (cần `KIBANA_HOST`; trên cụm, `deploy_all.sh` đã tự chạy).
  - Panel là Vega, định nghĩa trong `dashboards/setup_kibana.py`.
  - Bản export nằm ở `dashboards/kibana/steam_dashboards.ndjson`; có thể import tay qua Stack Management → Saved Objects.
- Grafana D5: `dashboards/grafana/d5_system.json`.
  - Trên cụm được nạp qua ConfigMap có label `grafana_dashboard`.
  - Ở nơi khác, import tay qua Dashboards → Import.

**Khác biệt so với thiết kế:**
- D2 hiện từ khoá TF-IDF bằng word cloud (Vega).
- Mốc cập nhật/giảm giá xem ở panel "tác động giảm giá" của D4.
- D2 lọc theo game bằng ô tìm kiếm: `appid:<id>`.

---

## 1. D1: Tổng quan thị trường (Kibana)
* **Mục đích:** Cung cấp bức tranh tổng quát về xu hướng và phản ứng của người chơi trên toàn bộ các tựa game được theo dõi.
* **Các biểu đồ chính:**
  * **Line chart:** Số lượng review theo thời gian (ngày/tuần).
  * **Bar chart:** Tỷ lệ % đánh giá tích cực theo thể loại game (Action, RPG, Strategy, Indie...).
  * **Horizontal Bar / Leaderboard:** Top game có lượng người chơi đồng thời (CCU) cao nhất.
  * **Pie / Donut chart:** Phân bố review theo ngôn ngữ (English, Schinese, Russian, Vietnamese...).
  * **Heatmap:** Phân tích tương quan giữa Khoảng giá bán (Price) và Tỷ lệ hài lòng (% positive).

---

## 2. D2: Chi tiết game (Kibana)
* **Mục đích:** Dành cho nhà phát triển / nhà phát hành muốn theo dõi chuyên sâu một tựa game cụ thể.
* **Các biểu đồ chính:**
  * **Timeline chart:** Tỷ lệ đánh giá tích cực theo tuần, có đánh dấu (annotation) các mốc cập nhật (patch notes) và đợt giảm giá (sales).
  * **Time series:** Biểu đồ dao động CCU theo từng khung giờ trong ngày và theo tuần.
  * **Stacked bar chart:** Tỷ lệ hài lòng theo từng nhóm giờ chơi (<2h, 2–10h, 10–50h, 50–200h, >200h).
  * **Radar / Bar chart:** Tỷ lệ phàn nàn phân theo khía cạnh (performance, bug, price, server, gameplay, story, cheating).
  * **Tag Cloud (Word Cloud):** Các từ khóa nổi bật được trích xuất bằng TF-IDF trong các bài review tiêu cực.

---

## 3. D3: Giám sát thời gian thực (Kibana / Realtime View)
* **Mục đích:** Theo dõi diễn biến tức thời từ luồng streaming.
* **Các biểu đồ chính:**
  * **Metric counter:** Số bài review mới được đăng tải trong mỗi giờ gần nhất.
  * **Gauge / Line chart:** Tỷ lệ hài lòng trung bình trong cửa sổ 60 phút gần nhất.
  * **Realtime Table:** Danh sách top game có CCU hiện tại cao nhất (cập nhật mỗi 5 phút).

---

## 4. D4: Cảnh báo & Xu hướng (Kibana)
* **Mục đích:** Cảnh báo sớm các sự kiện bất thường và phát hiện game tiềm năng.
* **Các biểu đồ chính:**
  * **Alert Table:** Danh sách các sự kiện nghi vấn *Review Bombing* (số review tiêu cực vượt ngưỡng trung bình + 3σ).
  * **Anomaly Table:** Cảnh báo đột biến tăng / sụt giảm mạnh số người chơi CCU (>50% so với ngày trước).
  * **Trending Board:** Bảng xếp hạng các game đang có tốc độ tăng trưởng CCU và review cao nhất tuần.

---

## 5. D5: Giám sát hệ thống (Grafana)
* **Mục đích:** Giám sát tài nguyên hạ tầng, phục vụ kiểm thử Scalability và Fault-Tolerance.
* **Các chỉ số chính:**
  * **Kafka Consumer Lag:** Độ trễ tiêu thụ message của các consumer group (`batch-ingest`, `spark-streaming`).
  * **Kafka Throughput:** Số lượng message/giây và MB/giây nhận vào cluster.
  * **HDFS Health:** Dung lượng sử dụng, số DataNodes khả dụng (Live/Dead), số block missing/under-replicated.
  * **Kubernetes Resources:** Tỷ lệ sử dụng CPU & RAM của từng Pod (Kafka brokers, Spark executors, Elasticsearch nodes).
