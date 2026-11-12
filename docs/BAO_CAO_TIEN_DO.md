# BÁO CÁO TIẾN ĐỘ DỰ ÁN STEAM PLAYER INSIGHTS

**Đề tài:** Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam.
**Môn học:** Lưu trữ và xử lý dữ liệu lớn.
**Hướng thực hiện:** Direction 1: xây dựng và cài đặt kiến trúc dữ liệu lớn.
**Ngày đối chiếu:** 12/11/2026 (thay bản ngày 11/11/2026).
**Phạm vi:** mã nguồn, dữ liệu và kết quả chạy có trong workspace của dự án tại ngày đối chiếu.

## 1. Nội dung báo cáo nhanh với giảng viên

Nhóm đã xây dựng đủ các tầng của hệ thống theo kiến trúc Lambda: thu thập, lưu trữ raw/clean/curated, xử lý batch, xử lý streaming, truy vấn và dashboard. Ngày 12/11/2026, **toàn bộ luồng đã chạy thông trên một máy với dữ liệu thật**: Steam API → Kafka → Spark (8 job batch và 3 job streaming) → Elasticsearch → Kibana D1–D4.

Ngày 12/11/2026, nhóm **thay toàn bộ bộ dữ liệu** để chỉ dùng API chính thức của Steam. Bộ cũ khoảng 1,1 triệu review được trộn từ dataset Hugging Face, dữ liệu crawl và SteamSpy, không ghi rõ nguồn gốc từng phần. Vì vậy các số liệu của bản báo cáo ngày 11/11 **không còn dùng được**. Bộ dữ liệu mới nhỏ hơn nhưng nguồn gốc rõ, phục vụ demo:

- **100 game**, chọn có phương pháp: 20 game top CCU và 80 game lấy mẫu phân tầng theo số review.
- **92.019 review**, không trùng ID; 100/100 game đều có review.
- Metadata, tag và **số review theo ngày do Steam công bố** cho cả 100 game.
- 200 snapshot CCU (2 lần thu).

Việc thu tiếp đã có cơ chế **tăng dần, không trùng**, nên khi nhóm mở rộng lên 1.000 game hoặc hơn, dữ liệu đã thu được giữ nguyên.

Phần **chưa có bằng chứng nghiệm thu**:
- Chạy trên cụm Kubernetes nhiều node.
- Số đo scalability và fault-tolerance chính thức (mới chạy thử script trên một máy).
- Độ trễ streaming đo theo chuẩn ≤ 2 phút.
- Lịch sử CCU nhiều ngày: vòng thu CCU định kỳ mới bắt đầu.

**Trạng thái chung phù hợp để báo cáo:** phần mềm đủ các tầng, đã chạy toàn trình trên dữ liệu thật ở quy mô demo; đang chuẩn bị thu dữ liệu liên tục, mở rộng quy mô và chạy kiểm thử trên cụm.

## 2. Căn cứ và cách xác định tiến độ

### 2.1. Tài liệu được đối chiếu

| Tài liệu | Vai trò khi đánh giá |
| --- | --- |
| [Projects.pptx.pdf](../Projects.pptx.pdf) | Yêu cầu môn học: đủ các tầng, dashboard hữu ích, phân tích dữ liệu, môi trường k8s/cloud, mô phỏng phân tán, scalability và fault-tolerance |
| [BigData.pdf](../BigData.pdf) | Thiết kế gốc: F1–F6, Lambda, B1–B6, S1–S3, D1–D5, SC1–SC4, FT1–FT6 và sản phẩm bàn giao |
| [PLAN.md](PLAN.md) | Trạng thái theo giai đoạn và việc còn lại (cập nhật 12/11/2026) |
| [docs/decisions.md](decisions.md) | Quyết định triển khai, nguồn dữ liệu chỉ từ Steam, cấu hình thu gọn |
| [docs/collection_guide.md](collection_guide.md) | Endpoint, cách chọn game, cơ chế chống trùng, lịch thu |
| [docs/reports/collection_trial30_audit_20261112.md](reports/collection_trial30_audit_20261112.md) | Kiểm toán lần crawl thử 30 game, lý do đổi cách chọn game |
| [docs/architecture.md](architecture.md) | Luồng dữ liệu, schema, baseline, hợp nhất view |
| [README.md](../README.md) mục 4.3 và 4.11 | Cách lấy dữ liệu và kịch bản demo toàn hệ thống trên một máy |
| [van_de_va_giai_phap_ha_tang.md](van_de_va_giai_phap_ha_tang.md) | Phân tích tài nguyên và các điều chỉnh đã áp dụng |

### 2.2. Quy ước trạng thái

| Trạng thái | Ý nghĩa |
| --- | --- |
| Đã chạy, có kết quả | Đã chạy trong ngày đối chiếu; có output, log hoặc số đếm kiểm tra lại được |
| Có triển khai | Có code/cấu hình, chưa chạy trong ngày đối chiếu hoặc chưa chạy ở môi trường yêu cầu |
| Chạy thử một máy | Script chạy thành công trên một máy; **không** thay cho số đo trên cụm nhiều node |
| Chưa đủ bằng chứng nghiệm thu | Thiếu log, số đo hoặc môi trường đáp ứng điều kiện của thiết kế |

Mọi số liệu dưới đây được đếm trực tiếp từ `data/raw` (JSON Lines gzip), `data/lake` (Parquet), Elasticsearch và `tests/results/` vào ngày 12/11/2026.

## 3. Kiến trúc đang chạy

Hệ thống trả lời năm nhóm câu hỏi: mức hài lòng thay đổi theo thời gian; người chơi phàn nàn khía cạnh nào; quan hệ giữa giờ chơi và đánh giá; bất thường review/CCU; và game nào đang tăng trưởng.

```text
Steam API: AppList · GetItems (tag, tổng review) · Most Played · appdetails
           · review histogram (review/ngày) · GetAppReviews · CCU
                                  |
                 Python crawlers (chọn game, crawl tăng dần)
                                  |
              +-------------------+--------------------+
              |                                        |
      File JSONL gzip (data/raw)                     Kafka
              |                               /               \
              |                        B1 -> raw          S1 / S2 / S3
              +------------> B2 -> clean                       |
                               |                               |
                 B5 -> B3 -> B4 -> B6 -> B7 -> B8              |
                               |                               |
                            curated                  realtime / alerts
                               \                              /
                            Elasticsearch + merge_views (cutoff)
                                          |
                               Kibana D1–D4 · Spark SQL

     Vòng phản hồi: game đang lên trên bảng most-played -> thêm vào danh sách theo dõi
     Prometheus + Grafana D5: chỉ có trên cụm Kubernetes
```

[pipeline.py](../pipeline.py) là điểm chạy chung. Lệnh `batch` chạy **B2 → B5 → B3 → B4 → B6 → B7 → B8**, kèm `merge_views` khi ghi ES. B1 được gọi riêng để lấy dữ liệu từ Kafka.

## 4. Đối chiếu các yêu cầu chức năng F1–F6

| Mã | Yêu cầu thiết kế | Kết quả ngày 12/11/2026 | Còn lại |
| --- | --- | --- | --- |
| F1 | Crawl định kỳ review, metadata, CCU; Kafka; backfill | **Đã chạy, có kết quả.** 8 endpoint Steam; crawl review tăng dần không trùng; crawler đẩy thẳng vào Kafka khi demo streaming | Vòng thu định kỳ chạy liên tục ≥ 14 ngày; CronJob trên cụm cho nguồn `store`/`histogram` |
| F2 | Raw bất biến trên HDFS; clean Parquet | **Đã chạy local.** Raw JSONL gzip 18,3 MiB; clean 5 bảng | Chưa chạy HDFS trong ngày đối chiếu; chưa có raw trên HDFS của cụm |
| F3 | Batch tổng hợp, cảm xúc, khía cạnh, xu hướng | **Đã chạy, có kết quả.** B2–B8 thành công; trending dùng số review/ngày của Steam | Lịch chạy hằng ngày trên cụm; kiểm tra khi tăng quy mô |
| F4 | Streaming cửa sổ, bất thường, cảnh báo trong vài phút | **Đã chạy local với Kafka.** S1–S3 xử lý review/CCU thật; cảnh báo xuất hiện trong khoảng 1 phút sau khi bơm tình huống | Chưa đo độ trễ end-to-end theo chuẩn; chưa chạy trên cụm |
| F5 | SQL ad-hoc trên Parquet/Hive; tìm kiếm review trong ES | **Có triển khai.** `reviews_search` 34.725 review; Spark SQL đọc Parquet | Hive Metastore chưa triển khai (cấu hình thu gọn); cần lưu ví dụ truy vấn thật |
| F6 | Tổng quan, chi tiết game, realtime, cảnh báo | **Đã chạy.** 18/19 panel Vega của D1–D4 trả dữ liệu khi chạy truy vấn của panel trên ES | Ảnh chụp dashboard; D5 chỉ có trên cụm |

## 5. Thu thập và chất lượng dữ liệu

### 5.1. Thay đổi ngày 12/11/2026

1. **Chỉ dùng API chính thức của Steam.** Không dùng Hugging Face, Kaggle hay SteamSpy, kể cả để chọn game. Tag lấy từ `IStoreBrowseService/GetItems`. Giờ chơi lấy từ từng review. Quy mô game dùng tổng review thay cho số người sở hữu ước lượng của SteamSpy.
2. **Đổi cách chọn game.** Lần crawl thử lấy 30 game ngẫu nhiên đều trên catalog: 13/30 game không có review, 11/30 game chưa phát hành (CCU lỗi 404), 5 game chiếm 89,7% review. Nhóm khảo sát 400 game ngẫu nhiên và thấy khoảng 70% catalog có dưới 20 review, 29% chưa phát hành. Cách chọn mới (`crawlers/build_targets.py`):
   - Nhóm `top_ccu`: game đầu bảng most-played chính thức của Steam.
   - Nhóm `stratified`: lấy mẫu có seed sau khi lọc game đã phát hành và có ≥ 20 review, chia đều 4 tầng số review: 20–99, 100–999, 1k–9,9k, ≥ 10k.
3. **Crawl tăng dần, không trùng.**
   - Mỗi lượt chỉ lấy review mới kể từ lượt trước, rồi lấy tiếp review cũ từ chỗ đã dừng.
   - Danh sách game chỉ được mở rộng, không xáo lại, nên lần thu lớn sau dùng lại toàn bộ dữ liệu đã có.
   - Cơ chế này được kiểm thử với 13.000 kịch bản mô phỏng (lỗi mạng, hết ngân sách request, review trùng giây): không có review nào bị ghi hai lần hay bị bỏ sót.
4. **Thêm nguồn số review theo ngày** (`appreviewhistogram`). Đây là số đếm đầy đủ của Steam, dùng cho trending (B6) và baseline (B7). Review đã crawl ở game lớn chỉ phủ vài giờ đến vài ngày gần nhất, nên không dùng để đếm.
5. **Vòng phản hồi:** thêm game đang lên trên bảng most-played (mới vào top hoặc tăng hạng so với tuần trước). Chỉ thêm, không xoá; tạm tắt trong giai đoạn demo để giữ cố định 100 game.

### 5.2. Số liệu hiện có

| Hạng mục | Số liệu | Diễn giải |
| --- | ---: | --- |
| Game theo dõi | 100 | 20 `top_ccu` + 80 `stratified` (20 mỗi tầng); manifest ghi seed, ngưỡng, thời điểm |
| Game có review | 100 | Lần thử trước: 17/30 |
| Review raw | 92.019 | 92.019 ID phân biệt, **0 trùng** qua hai lượt crawl |
| Review tiếng Anh | 39.337 | Chỉ review tiếng Anh được phân tích nội dung |
| Khoảng thời gian review | 10/09/2013 – 12/11/2026 | 28.414 review trong 7 ngày gần nhất, thuộc 61 game |
| Mức độ tập trung | Game lớn nhất 2,2%; 5 game lớn nhất 10,6% review clean | Lần thử trước: 5 game chiếm 89,7% |
| Snapshot metadata | 100 appdetails + 100 store + 100 histogram | Mỗi game đủ 3 loại |
| Số review theo ngày (histogram) | 2.950 dòng | 30 ngày gần nhất mỗi game |
| Snapshot CCU | 200 | 2 lần thu (3h và 5h UTC ngày 12/11) |
| Review clean | 87.407 | B2 lọc review rỗng/spam tiếng Anh, giữ review ngôn ngữ khác để dùng nhãn |
| Tỷ lệ tích cực (clean) | 80,03% | Tỷ lệ của mẫu 100 game, không đại diện toàn Steam |
| Dung lượng raw / lake | 18,3 MiB / 165,8 MiB | Raw nén gzip; lake gồm nhiều bảng dẫn xuất |

### 5.3. Hạn chế cần nêu

1. **Quy mô demo:** 100 game và 92 nghìn review nhỏ hơn mục tiêu thiết kế (1.000–2.000 game, hàng triệu review). Đây là lựa chọn có chủ đích để demo; việc mở rộng chỉ cần chạy lại `build_targets --limit` lớn hơn.
2. **CCU mới có 2 snapshot mỗi game:** chưa tính được xu hướng CCU tuần; S3 chỉ có baseline cho 2 khung giờ. Cần chạy vòng thu CCU liên tục ≥ 14 ngày.
3. **Review của game lớn chỉ phủ gần đây:** mỗi lượt lấy tối đa 1.000 review/game, các lượt sau lấy tiếp. Phân tích nội dung review của game lớn là mẫu gần nhất, không phải toàn bộ lịch sử.
4. **Mẫu không đại diện toàn catalog:** game dưới 20 review và game chưa phát hành bị loại có chủ đích; nhóm `top_ccu` là game phổ biến.
5. **Giá mới có 1 snapshot:** chưa quan sát được đợt giảm giá nào bắt đầu, nên bảng tác động giảm giá đang trống.

## 6. Tiến độ xử lý batch B1–B8

Lần chạy toàn bộ ngày 12/11/2026: `python pipeline.py --profile local batch --raw data/raw --lake data/lake --es`, Spark `local[4]`, driver 2–3 GB, **kết thúc thành công** (khoảng 12 phút, B5 chậm nhất).

| Job | Nội dung | Kết quả trên dữ liệu thật | Ghi chú |
| --- | --- | --- | --- |
| B1 — Ingest raw | Đọc Kafka theo offset, ghi JSONL gzip, lưu offset | Đã chạy với Kafka local, ghi raw riêng `data/kafka_demo_raw` | Chưa chạy Kafka → HDFS trên cụm |
| B2 — Clean | Khử trùng review, làm sạch CCU/metadata, gắn cờ đáng ngờ; tách tag (store) và số review/ngày (histogram); loại sự kiện demo | 87.407 review, 200 CCU, 100 game, 2.950 dòng review/ngày | Đánh dấu ngày đang đếm dở (`is_partial`) |
| B3 — Tổng hợp | Game/ngày, tuần/tháng, phân khúc, ngôn ngữ | 19.861 game/ngày; 9.565 game/kỳ; 4.780 phân khúc; 14.701 ngôn ngữ/ngày | |
| B4 — Giờ chơi | 5 nhóm giờ chơi, tiêu cực sớm, điểm đáng tiền | 433 dòng nhóm giờ chơi; 100 hồ sơ game | Tiêu cực sớm là chỉ báo gián tiếp |
| B5 — Cảm xúc/khía cạnh | VADER, từ điển khía cạnh, TF-IDF | 34.725 review có cảm xúc; 6.266 dòng khía cạnh; 2.812 từ khoá | Chưa đánh giá độ chính xác |
| B6 — Xu hướng/sự kiện | 7 ngày so với 7 ngày trước; z-score; tác động giảm giá | 21.742 dòng trending; top tại ngày đủ dữ liệu 11/11 | Ví dụ: ROB RUN 21.899 review/7 ngày so với 637 tuần trước |
| B7 — Baseline | Review/giờ (cho S2), CCU cùng khung giờ (cho S3) | 100 baseline review; 200 baseline CCU | Xem 6.1 |
| B8 — Phân cụm | K-Means 6 cụm | 100 game; cỡ cụm 7, 53, 3, 10, 1, 26 | Cụm 1 game cần xem lại đặc trưng trước khi diễn giải |

### 6.1. Thay đổi logic so với bản 11/11 (đều phát hiện khi chạy trên dữ liệu thật)

- **B6 đếm review từ histogram của Steam** thay vì đếm review đã crawl, để không đánh giá thấp game lớn. Xếp hạng tại ngày đủ dữ liệu cuối cùng (không dùng ngày đang đếm dở). Game có dưới 10 review/tuần xếp cuối bảng.
- **B6 tác động giảm giá:** chỉ tính "bắt đầu sale" khi quan sát được bước chuyển từ không giảm sang giảm. Bản cũ coi game đang giảm ở snapshot đầu tiên là bắt đầu sale; 722 dòng của bản 11/11 thuộc trường hợp này.
- **B7 baseline review:** dùng 7 ngày đủ dữ liệu của histogram. Độ lệch chuẩn theo giờ ước lượng bằng √(trung bình + phương sai giữa các ngày / 24²). Nếu cửa sổ có dưới 30 review, tỷ lệ tích cực lấy theo tỷ lệ trọn đời Steam công bố. Thay đổi này loại được báo động giả ở game ít review.

### 6.2. Kết quả phân tích ban đầu có thể trình bày

- **Trending:** ROB RUN (game mới ra) tăng 3.338% số review; Probably Stolen tăng 67%; Brotato tăng 33%. Đây là số review do Steam đếm, không phụ thuộc giới hạn crawl.
- **Mức hài lòng:** 80,03% review clean là tích cực. Tỷ lệ này phụ thuộc cách chọn mẫu, không đại diện cho toàn Steam.
- **Khía cạnh, giờ chơi, phân cụm:** đã có bảng trên 100 game. Cần đọc mẫu review và hồ sơ cụm trước khi đưa ra nhận xét.

B2–B8 tính lại từ nguồn và ghi đè; ES dùng ID ổn định. B1 ghi raw trước rồi mới lưu offset, nên sự cố giữa hai bước có thể làm ghi lặp; B2 khử trùng. Hệ thống **không** bảo đảm exactly-once ở mọi bước.

## 7. Streaming và hợp nhất Lambda

### 7.1. Lần chạy ngày 12/11/2026

Môi trường: Kafka 1 broker, Elasticsearch và Kibana chạy bằng Docker Compose (`compose.demo.yaml`); S1–S3 chạy Spark local, đọc Kafka, ghi ES. Lệnh: `bash scripts/demo.sh stream-start`.

| Nguồn đưa vào Kafka | Kết quả |
| --- | --- |
| CCU thật của 100 game, gọi Steam trực tiếp | S3 ghi CCU hiện tại vào `realtime_game_metrics` |
| 3.501 review thật trong 24 giờ qua, crawler đẩy thẳng vào Kafka | S1 ghi số review và tỷ lệ tích cực theo cửa sổ 5 phút và 60 phút; S2 **không** báo động giả |
| Bơm có nhãn: 80 review tiêu cực cho Counter-Strike 2 | S2 cảnh báo `REVIEW_BOMBING`: 44 review tiêu cực trong cửa sổ 60 phút, vượt ngưỡng 28,8; tỷ lệ tích cực 48,2% so với baseline 81,9% |
| Bơm có nhãn: CCU gấp 3 lần baseline cùng khung giờ | S3 cảnh báo `CCU_SPIKE`, change_ratio = 2,0 |

Ngoài hai tình huống bơm, S2 phát hiện một cụm review tiêu cực **thật** của Overwatch: 10 review tiêu cực trong một giờ so với mức thường 2,4 ± 1,6, tỷ lệ tích cực 23% so với 52%. Đây là dấu hiệu thống kê cần điều tra, chưa phải kết luận có review bombing. Luật 3σ trên số đếm nhỏ thỉnh thoảng sẽ bắt những cụm như vậy; ngưỡng chỉnh được qua biến môi trường.

Sự kiện bơm (`crawlers/demo_inject.py`) mang `source=demo_injection`. B2 loại các bản ghi này, nên chúng không lẫn vào dữ liệu phân tích.

### 7.2. Lỗi đã sửa trong lần chạy này

- **Lệch múi giờ khi ghi ES:** thời gian realtime bị ghi lệch 7 giờ trên máy đặt múi giờ Việt Nam, nên D3/D4 trống. Đã đổi sang UTC khi ghi.
- **Xoá nhầm realtime khi hợp nhất:** điều kiện ngày không kèm giờ bị ES làm tròn lên cuối ngày, nên mỗi lần chạy batch xoá cả realtime của hôm nay. Giờ cutoff là thời điểm chính xác và không vượt 00:00 UTC hôm nay. Bước này đã được kiểm tra: xoá 4.576 cửa sổ của hôm qua (batch đã bao phủ), giữ 2.690 cửa sổ của hôm nay.
- **Kafka trong compose không khởi động được** (quyền ghi volume).
- **Báo động giả ở game ít review** (xem 6.1).

### 7.3. Còn lại

- Đo độ trễ end-to-end (thời điểm nhập sự kiện → thời điểm thấy trên ES) theo yêu cầu ≤ 2 phút. Mới quan sát cảnh báo xuất hiện trong khoảng 1 phút với trigger 20 giây; đây chưa phải số đo.
- S3 cần baseline CCU cho đủ 24 khung giờ, tức là cần vòng thu CCU chạy ít nhất 24 giờ.
- Chạy S1–S3 trên cụm, khôi phục từ checkpoint khi mất driver (FT3).
- Dashboard chưa đọc `serving_state` để hiển thị liền mạch quanh cutoff; D1/D2 đọc batch, D3 đọc realtime.

## 8. Lưu trữ, truy vấn và dashboard

### 8.1. Elasticsearch

Mapping chuẩn có 15 index ([indices_mapping.json](../storage/elasticsearch/indices_mapping.json)). Số document sau lần chạy ngày 12/11/2026 (ES 1 node, Docker):

| Index | Số document |
| --- | ---: |
| `batch_game_daily` | 19.861 |
| `batch_trending` | 21.742 |
| `batch_language_stats` | 14.701 |
| `batch_game_period` | 9.565 |
| `batch_aspect_stats` | 6.266 |
| `batch_segment_stats` | 4.780 |
| `reviews_search` | 34.725 |
| `game_keywords` | 2.812 |
| `batch_playtime_buckets` | 433 |
| `batch_game_retention`, `batch_game_clusters` | 100 mỗi index |
| `realtime_game_metrics`, `alerts` | Thay đổi theo phiên streaming |

Hive Metastore chưa triển khai (cấu hình thu gọn). Truy vấn ad-hoc dùng Spark SQL trên Parquet.

### 8.2. Dashboard D1–D5

Cách kiểm tra: chạy chính truy vấn ES của từng panel Vega (thay biến của Kibana bằng khoảng thời gian mặc định của dashboard), rồi đếm kết quả. Cách này xác nhận panel có dữ liệu, nhưng **chưa thay cho ảnh chụp dashboard đã render**.

| Dashboard | Kết quả | Ghi chú |
| --- | --- | --- |
| D1 — Tổng quan | 9/9 panel có dữ liệu | KPI số game là cardinality ước lượng |
| D2 — Chi tiết game | 5/5 panel có dữ liệu | Lọc `appid:<id>`; chưa có annotation cập nhật trên timeline |
| D3 — Realtime | 3/3 panel có dữ liệu khi streaming chạy | Khoảng thời gian mặc định 24 giờ |
| D4 — Cảnh báo, xu hướng | Cảnh báo và trending có dữ liệu; tác động giảm giá trống | Trống đúng: chưa có 2 snapshot giá quan sát được bước chuyển |
| D5 — Hệ thống (Grafana) | Chưa chạy | Cần Prometheus; chỉ có trên cụm và chưa triển khai trong cấu hình thu gọn |

## 9. Hạ tầng Kubernetes

Không thay đổi so với bản 11/11: đã có manifest Kafka (Strimzi/KRaft), HDFS, ECK, Spark CronJob/Deployment, crawler CronJob, script dựng cụm k3s và kiểm thử lỗi. **Chưa chạy trên cụm thật.** Kế hoạch theo [docs/decisions.md](decisions.md): 4 VM `e2-standard-2` trên Google Cloud, k3s, nối bằng Tailscale, chỉ bật theo buổi.

Việc phải làm trước khi lên cụm, do code đã thay đổi:
- Build lại image Spark và tăng tag: tag hiện tại `k3s-20261026` chưa có các thay đổi ngày 12/11.
- Thêm CronJob cho nguồn `store`, `histogram` và vòng phản hồi.
- Đưa `data/target_apps.json` và state crawler lên cụm, để crawl tiếp không trùng.

| Thành phần | Thiết kế gốc | Cấu hình thu gọn | Hệ quả |
| --- | --- | --- | --- |
| Kubernetes | Nhiều node, ≥ 3 worker | 1 server + 3 agent | Chưa có log `nodes` và vị trí pod |
| Kafka | 3 broker, RF=3, min ISR=2 | Giữ nguyên | Đủ cho FT1 |
| HDFS | 1 NameNode + 3 DataNode, replication 3 | 1 + 2, replication 2 | Chưa HA NameNode |
| Elasticsearch | 3 node, 1 replica | 1 node, 0 replica | Chưa đáp ứng FT4/SC4 |
| Hive Metastore | Có | Chưa triển khai | Cần giảng viên xác nhận |
| Monitoring | Prometheus + Grafana | Chưa triển khai | D5 chưa vận hành |

## 10. Kiểm thử

### 10.1. Unit test

`python -m pytest -q tests/unit`: **36/36 đạt** ngày 12/11/2026. Các nhóm test:
- **Thu thập:** chọn game phân tầng, mở rộng danh sách không trùng, crawl tăng dần (gồm 30 kịch bản ngẫu nhiên; bản kiểm tra riêng chạy 13.000 kịch bản).
- **Xử lý:** trending và baseline từ histogram, loại sự kiện demo ở B2, luật cảnh báo S2/S3.
- **Serving:** ghi thời gian UTC vào ES, cutoff khi hợp nhất.
- **Còn lại:** tiền xử lý văn bản, khía cạnh, vòng phản hồi.

### 10.2. Scalability SC1–SC4

| Kịch bản | Trạng thái | Kết quả chạy thử một máy (`tests/results/`) |
| --- | --- | --- |
| SC1 — Tăng executor | Chạy thử một máy | 11.637 review mẫu: B2–B5 mất 74,8 giây với 1 core, 73,9 giây với 4 core. Ở quy mô này, thời gian khởi động Spark chiếm phần lớn nên **chưa thấy hiệu quả mở rộng**. Cần dữ liệu lớn hơn và executor phân tán trên cụm |
| SC2 — Tăng dữ liệu | Chưa chạy | Script hỗ trợ 1×/2×/5×/10× |
| SC3 — Tải streaming | Chạy thử một máy | 200 và 1.000 review/giây: S1 xử lý trung bình 1.180 và 5.925 dòng/giây; micro-batch trung bình 1,4 và 1,3 giây (p95 2,5 và 1,9 giây); không tụt offset |
| SC4 — Truy vấn ES | Chạy thử 1 node | 4 luồng: 303 truy vấn/giây, p50 5,3 ms, p95 64 ms. 16 luồng: 474 truy vấn/giây, p50 12,9 ms, p95 66 ms. 0 lỗi. Kịch bản thật cần so ES 2 với 3 node |

Thời gian micro-batch và offset lag chưa phải độ trễ end-to-end. Với dữ liệu phát lại (`--retime`), không lấy chênh lệch thời gian gốc của review làm độ trễ của pipeline.

### 10.3. Fault-tolerance FT1–FT6

| Kịch bản | Trạng thái |
| --- | --- |
| FT1 — Mất Kafka broker | Có script và observer kiểm tra bản ghi đã ACK; cần cụm |
| FT2 — Mất DataNode | Có kịch bản; cần cụm, kiểm tra đủ bản sao trước khi gây lỗi |
| FT3 — Mất executor/driver | Có kịch bản; cần cụm, đối soát ID sau khi khôi phục |
| FT4 — Mất node ES | Cần ES nhiều node và replica |
| FT5 — Mất worker | Cần cụm; nên tắt VM thật thay vì chỉ drain |
| FT6 — API lỗi 429 | **Đạt ngày 12/11/2026:** crawler giãn nhịp khi gặp 429, không dừng; lượt 1 thu 5/8 game, lượt 2 thu đủ 8/8 |

## 11. Notebook và sản phẩm bàn giao

| Sản phẩm | Hiện trạng | Việc còn lại |
| --- | --- | --- |
| Mã nguồn các tầng | Đủ; đã chạy toàn trình local | Build image mới, chạy trên cụm |
| Hướng dẫn chạy | README mục 4.3 (lấy dữ liệu), 4.11 (demo toàn hệ thống); `collection_guide.md` | Cập nhật hướng dẫn cụm khi chạy xong |
| Notebook EDA, cảm xúc, SC/FT | Có code, chưa chạy trên bộ dữ liệu mới | Chạy, lưu hình và nhận xét |
| Gói dữ liệu cho nhóm | `steam_data_20261112.tar.gz` (19 MB) | Tải lên Drive |
| Báo cáo cuối kỳ, slide, video | Chưa có | Sau khi có kết quả cụm và phân tích |

## 12. Khó khăn và hướng xử lý

| Khó khăn | Ảnh hưởng | Hướng xử lý |
| --- | --- | --- |
| CCU không có API lịch sử | Phải tự thu dần; máy tắt là mất dữ liệu khoảng đó | Chạy vòng thu liên tục trên một máy bật 24/7, sau chuyển sang VM hoặc CronJob trên cụm |
| Giới hạn tốc độ Steam (khoảng 1 request/1,5 giây) | 1.000 game mất 4–6 giờ cho lượt đầu; chu kỳ CCU dài hơn 10 phút | Chia worker trên cụm; ưu tiên CCU trong vòng lặp |
| RAM máy cá nhân khoảng 8 GB | Không chạy cùng lúc batch, streaming, ES/Kibana và Kafka | Bật theo nhóm; dừng streaming khi chạy batch |
| Bộ dữ liệu cũ trộn nguồn | Số liệu cũ không dùng được | Đã thay bằng dữ liệu chỉ từ Steam, có manifest |
| Quá nhiều file nhỏ | Raw có 1.020 file review cho 92 nghìn review; `clean/reviews` có 10.737 file Parquet. Tăng chi phí liệt kê/đọc khi mở rộng và trên HDFS | Gộp raw theo ngày; B2 `repartition` theo `dt` trước khi ghi; làm trước khi mở rộng |
| Luật 3σ trên số đếm nhỏ | Thỉnh thoảng có cảnh báo ở game nhỏ | Ngưỡng `S2_SIGMA`, `S2_MIN_NEGATIVE` chỉnh được; báo cáo tỷ lệ cảnh báo theo thời gian |

## 13. Kế hoạch tiếp theo

| Ưu tiên | Công việc | Điều kiện hoàn thành |
| --- | --- | --- |
| P0 | Chạy vòng thu CCU/review/giá liên tục | ≥ 14 ngày CCU; baseline S3 đủ 24 khung giờ |
| P0 | Gộp file nhỏ ở raw và clean | Số file theo ngày thay vì theo game/nguồn |
| P0 | Mở rộng danh sách game (ví dụ 1.000 game) | Crawl tăng dần, không trùng; báo cáo độ phủ từng tầng |
| P0 | Build image, chạy toàn trình trên cụm 4 VM | Log `nodes`, vị trí pod, số document ES, ảnh D1–D4 |
| P1 | SC1–SC4 trên cụm | CSV, biểu đồ speedup, throughput, độ trễ |
| P1 | FT1–FT5 trên cụm | Log trước/trong/sau lỗi, đối soát dữ liệu, video |
| P1 | Đo độ trễ end-to-end streaming | Số đo so với yêu cầu ≤ 2 phút |
| P1 | Chạy notebook, trả lời 5 câu hỏi nghiệp vụ | Mỗi nhận xét có số liệu, khoảng thời gian, cỡ mẫu và hạn chế |
| P2 | Bật vòng phản hồi gần ngày bảo vệ | Có game `feedback` được thêm và crawl |
| P2 | Báo cáo, slide, video | Người khác chạy lại được theo tài liệu |

## 14. Kịch bản trình bày với giảng viên

### 14.1. Trình tự 8–10 phút

1. **Mục tiêu và kiến trúc (1 phút):** 5 câu hỏi nghiệp vụ, luồng Lambda.
2. **Dữ liệu (1–2 phút):** vì sao chỉ dùng Steam; cách chọn 100 game; crawl tăng dần không trùng.
3. **Batch và dashboard (2–3 phút):** D1, D2 với `appid:730`, D4 trending (ROB RUN).
4. **Streaming trực tiếp (2 phút):** crawler đẩy CCU và review thật vào Kafka → D3 cập nhật. Bơm tình huống bombing → D4 có cảnh báo; nói rõ đó là dữ liệu bơm có nhãn và B2 loại chúng khỏi batch.
5. **Khoảng trống và kế hoạch (2 phút):** cụm, SC/FT, lịch sử CCU, độ trễ.

Các lệnh dùng khi demo nằm ở README mục 4.11.

### 14.2. Đoạn phát biểu có thể dùng

> Thưa cô/thầy, đến ngày 12/11, nhóm đã chạy thông toàn bộ hệ thống Lambda trên dữ liệu thật: crawler lấy dữ liệu trực tiếp từ API Steam, Kafka nhận luồng sự kiện, Spark chạy 8 job batch và 3 job streaming, kết quả đưa lên Elasticsearch và Kibana. Nhóm đã bỏ bộ dữ liệu trộn nguồn trước đây và chỉ dùng API chính thức của Steam: 100 game được chọn theo phương pháp phân tầng, hơn 92 nghìn review không trùng. Cơ chế thu thập cho phép mở rộng mà không lấy lại dữ liệu cũ. Phần nhóm cần làm tiếp là thu dữ liệu liên tục để có lịch sử CCU, mở rộng quy mô, chạy trên cụm Kubernetes nhiều node và đo khả năng mở rộng, khả năng phục hồi và độ trễ streaming.

## 15. Các điểm cần trao đổi với giảng viên

1. Dùng Spark SQL đọc Parquet trực tiếp thay Hive Metastore có đáp ứng F5 không?
2. HDFS replication 2 và ES 1 node/0 replica có được chấp nhận cho báo cáo tiến độ? Cấu hình nào phải nâng trước khi nghiệm thu FT4/SC4?
3. Yêu cầu "No docker": nhóm dùng Docker Compose để phát triển và demo trên một máy, còn nghiệm thu chạy trên k3s (containerd). Cách này có được chấp nhận không?
4. Quy mô tối thiểu khi chỉ dùng API Steam: bộ demo 100 game / 92 nghìn review; nhóm dự kiến mở rộng lên khoảng 1.000 game. Mức nào là đủ?
5. FT5 cần tắt VM thật hay chấp nhận drain node trong một số lượt thử?
