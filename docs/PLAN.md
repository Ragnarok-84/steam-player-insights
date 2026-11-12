# Kế hoạch hoàn thành repo Steam Player Insights

Cập nhật: 2026-11-12. Đối chiếu với thiết kế gốc `BigData.pdf`. Kế hoạch gốc ngày 2026-10-09 (đối chiếu code với thiết kế, giai đoạn 1–7) đã hoàn thành phần code; bản đầy đủ xem trong lịch sử git của file này.

## 1. Trạng thái (2026-11-12)

| Giai đoạn | Trạng thái | Đã kiểm chứng | Còn lại |
| --- | --- | --- | --- |
| 0 Thu thập | Xong, đã làm lại 12/11 | Crawl thật 100 game, 92.019 review, 0 trùng; FT6 đạt | Vòng thu liên tục; mở rộng danh sách; CronJob `store`/`histogram` |
| 1 Batch (B1–B8) | Xong | B2–B8 trên dữ liệu thật, ghi ES; B1 Kafka → raw local | Chạy trên cụm, lịch hằng ngày |
| 2 Speed + hợp nhất | Xong local | S1–S3 với Kafka, review/CCU thật và tình huống bơm; `merge_views` giữ realtime hôm nay | Đo độ trễ end-to-end; chạy trên cụm |
| 3 Truy vấn / phân tích bổ sung | Xong code | K-Means, review đáng ngờ, vòng phản hồi mới | Hive Metastore chưa triển khai (cấu hình thu gọn) |
| 4 Dashboard | D1–D4 xong | 18/19 panel trả dữ liệu thật (panel còn lại trống đúng) | Ảnh chụp; Grafana D5 chỉ trên cụm |
| 5 K8s | Có manifest + script | kubeconform 32/32 (26/10); **chưa chạy trên cụm thật** | Build image mới, CronJob crawler mới, dựng cụm 4 VM |
| 6 Kiểm thử | Unit + chạy thử một máy | Unit 36/36; FT6 đạt; SC1/SC3/SC4 chạy thử một máy | SC1–SC4 và FT1–FT5 trên cụm |
| 7 Bàn giao | Tài liệu cập nhật | README 4.3, 4.11; `collection_guide.md`; báo cáo tiến độ | Notebook trên dữ liệu mới, báo cáo cuối, slide, video |

## 2. Thay đổi từ 26/10 đến 12/11

### 2.1. Thu thập dữ liệu (làm lại)

- **Chỉ dùng API chính thức của Steam.** Đã xoá `steamspy_crawler.py`, `historical_backfill.py`, `public_datasets.py`, `kaggle_replay_producer.py`, `cursor_manager.py`. Bộ dữ liệu cũ trộn nguồn không dùng nữa.
- **Chọn game** (`build_targets.py`): nhóm `top_ccu` (bảng most-played) và nhóm `stratified` (lọc game đã phát hành, ≥ 20 review, chia đều 4 tầng số review). Danh sách chỉ mở rộng, không xáo lại.
- **Nguồn mới:**
  - `store` (`GetItems`): tag kèm trọng số, tổng review, early access.
  - `histogram`: số review theo ngày của Steam.
  - Thêm trường review: `refunded`, `comment_count`, `last_played`, giờ chơi trên Deck.
  - Thêm trường appdetails: DLC, achievement, số ngôn ngữ.
- **Crawl review tăng dần, không trùng** (`common/review_state.py`), kiểm thử với 13.000 kịch bản ngẫu nhiên.
- **Vòng phản hồi** (`update_targets_from_trending.py`): thêm game đang lên trên bảng most-played, chỉ thêm không xoá. Bản cũ dùng B6 nên không bao giờ phát hiện được game mới.

### 2.2. Xử lý, serving và demo

- **B2:** tách tag và số review/ngày; đánh dấu ngày đang đếm dở; loại sự kiện `source=demo_injection`.
- **B6:**
  - Trending đếm review bằng histogram.
  - Xếp hạng tại ngày đủ dữ liệu; game dưới 10 review/tuần xếp cuối.
  - "Bắt đầu sale" phải quan sát được bước chuyển.
- **B7:** baseline review từ histogram. Cửa sổ dưới 30 review thì tỷ lệ tích cực lấy theo tỷ lệ trọn đời của Steam.
- **`es_writer`:** ghi thời gian theo UTC (trước đây lệch theo múi giờ máy, D3/D4 trống).
- **`merge_views`:** cutoff là thời điểm chính xác, không vượt 00:00 UTC hôm nay (trước đây xoá nhầm realtime cả ngày).
- **Demo:**
  - `crawlers/demo_inject.py` (`pipeline.py inject`) bơm tình huống có nhãn.
  - `scripts/demo.sh` thêm `stream-start|stream-stop|stream-status|reset-kafka`.
  - Sửa Kafka trong `compose.demo.yaml` không khởi động được.

## 3. Việc còn lại

### 3.1. Dữ liệu (làm ngay)

1. **Bật vòng thu liên tục** (README mục 4.3, A6) trên một máy bật 24/7: CCU khoảng 10 phút một lần, review khoảng 30 phút, giá/tag/histogram hằng ngày. Mục tiêu ≥ 14 ngày để B6 tính xu hướng CCU và S3 có baseline đủ 24 khung giờ.
2. **Sau buổi báo cáo tiến độ, mở rộng danh sách:** `build_targets --limit 1000 --top-ccu 50 --seed 43`. Lượt đầu mất khoảng 4–6 giờ.
3. **Gần ngày bảo vệ:** bật vòng phản hồi (`update_targets_from_trending --top 5` mỗi ngày).
4. Chỉ một nơi crawl tại một thời điểm; khi chuyển máy hoặc lên cụm thì mang theo `data/crawler_state`.
5. **Trước khi mở rộng: xử lý file nhỏ.** Raw hiện có 1.020 file review cho 92 nghìn review; `clean/reviews` có 10.737 file Parquet. Cần gộp raw theo ngày và cho B2 `repartition` theo `dt` trước khi ghi.

### 3.2. Lên cụm (giai đoạn 5)

1. Build lại image Spark và crawler, tăng tag (hiện `k3s-20261026`, chưa có thay đổi 12/11), sửa tag trong mọi manifest.
2. Thêm CronJob cho `collect_local --sources store metadata histogram` (hằng ngày) và vòng phản hồi.
3. Dựng 4 VM GCP theo `infra/k3s/README.md`; chạy toàn trình: crawler → Kafka → B1 → HDFS → B2–B8 → ES, cùng S1–S3.
4. Lưu `kubectl get nodes`, `kubectl get pods -A -o wide`, số document ES, ảnh D1–D4.

### 3.3. Kiểm thử (giai đoạn 6)

1. **SC1/SC2** trên cụm với dữ liệu đủ lớn. Chạy thử một máy ở 11.637 review chưa thấy hiệu quả mở rộng vì thời gian khởi động Spark chiếm phần lớn.
2. **SC3:** tăng partition/executor; bổ sung đo độ trễ end-to-end (thời điểm nhập → thời điểm ghi ES).
3. **SC4:** ES 2 node so với 3 node, đo thời gian về green.
4. **FT1–FT5** theo `infra/k3s/FAULT_TEST.md`; lưu CSV, log trước/trong/sau lỗi, video.

### 3.4. Phân tích và bàn giao (giai đoạn 7)

1. Chạy 3 notebook trên bộ dữ liệu mới, lưu output.
2. Trả lời 5 câu hỏi nghiệp vụ bằng số liệu, kèm cỡ mẫu và hạn chế.
3. Ảnh dashboard, báo cáo cuối kỳ, slide, video demo (kịch bản ở README mục 4.11).

## 4. Phân công gợi ý

| Người | Phụ trách |
| --- | --- |
| A | Vòng thu liên tục, mở rộng danh sách, chất lượng dữ liệu |
| B | Batch, notebook, trả lời câu hỏi nghiệp vụ |
| C | Streaming, đo độ trễ, dashboard |
| D | Cụm k3s, image, CronJob, FT/SC |

Cả nhóm cùng làm bàn giao.
