# Jupyter Notebooks - Steam Player Insights

Thư mục chứa các sổ tay Jupyter phục vụ phân tích khám phá dữ liệu (EDA), thử nghiệm mô hình và trực quan hóa kết quả kiểm thử:

1. **`01_eda_raw_reviews.ipynb`**:
   - Khám phá tập dữ liệu raw reviews và phân bố độ dài, ngôn ngữ, nhãn đánh giá.
   - Phát hiện các mẫu review spam, meme, ký tự ASCII art cần xử lý trong Job B2.

2. **`02_sentiment_aspect_exploration.ipynb`**:
   - Thử nghiệm và tinh chỉnh từ điển từ khóa khía cạnh (aspect dictionary: bug, performance, price, server, story...).
   - Đánh giá phân bố điểm cảm xúc VADER so với nhãn `voted_up` của Steam.
   - Thử nghiệm TF-IDF trích xuất từ khóa nổi bật theo từng tựa game cụ thể.

3. **`03_scalability_fault_tolerance_analysis.ipynb`**:
   - Đọc dữ liệu log và kết quả benchmark từ `tests/scalability/` và Grafana Prometheus metrics.
   - Vẽ đồ thị Scalability (Speedup vs Số executor, Throughput vs Data Volume).
   - Vẽ biểu đồ thời gian phục hồi dịch vụ (Recovery Time) sau khi gây lỗi pod (Kafka broker, DataNode, ES node).
