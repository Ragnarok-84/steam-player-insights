# BÁO CÁO TIẾN ĐỘ DỰ ÁN STEAM PLAYER INSIGHTS

**Đề tài:** Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam.  
**Môn học:** Lưu trữ và xử lý dữ liệu lớn.  
**Hướng thực hiện:** Direction 1 — xây dựng và cài đặt kiến trúc dữ liệu lớn.  
**Ngày đối chiếu:** 11/11/2026.  
**Phạm vi:** Thiết kế, mã nguồn và dữ liệu hiện có trong workspace của dự án.

## 1. Nội dung báo cáo nhanh với giảng viên

Nhóm đã xây dựng các thành phần chính của hệ thống theo kiến trúc Lambda, gồm thu thập dữ liệu, lưu trữ raw/clean/curated, xử lý batch, xử lý streaming, truy vấn và dashboard. Mã nguồn đã có 8 job batch B1–B8, 3 job streaming S1–S3, cơ chế hợp nhất view và vòng phản hồi cập nhật danh sách game theo dõi.

Đối với dữ liệu thật, workspace hiện có **1.101.641 dòng review raw**, tương ứng **1.099.441 ID review phân biệt**. Các bảng Parquet kết quả đã lưu chứa **1.045.439 review sau làm sạch**, **380.136 review tiếng Anh đã được phân tích cảm xúc**, **32.738 dòng thống kê game/ngày** và **990 game được phân cụm**. Như vậy, tiến độ đã vượt qua giai đoạn chỉ viết code hoặc chạy dữ liệu mẫu: đã có đầu ra phân tích trên bộ dữ liệu thật để trình bày.

Phần chưa đủ bằng chứng để nghiệm thu là **vận hành toàn bộ pipeline trên cụm Kubernetes nhiều node**, **đo scalability**, **đo phục hồi khi lỗi** và **xác nhận độ trễ streaming ≤ 2 phút**. Repo có manifest, công cụ và kịch bản cho những phần này, nhưng tại thời điểm đối chiếu chưa có thư mục `tests/results/` chứa số liệu đo. Dữ liệu CCU hiện chỉ nằm trong một ngày, cũng chưa đủ để đánh giá xu hướng nhiều ngày.

**Trạng thái chung phù hợp để báo cáo:** đã có triển khai phần mềm và kết quả batch trên dữ liệu thật; đang hoàn thiện bằng chứng tích hợp phân tán, vận hành streaming và đánh giá hệ thống.

## 2. Căn cứ và cách xác định tiến độ

### 2.1. Tài liệu được đối chiếu

| Tài liệu | Vai trò khi đánh giá |
| --- | --- |
| [Projects.pptx.pdf](Projects.pptx.pdf) | Yêu cầu môn học: đủ các tầng, dashboard hữu ích, phân tích dữ liệu, môi trường k8s/cloud, mô phỏng phân tán, scalability và fault-tolerance |
| [BigData.pdf](BigData.pdf) | Thiết kế gốc: F1–F6, Lambda, B1–B6, S1–S3, D1–D5, SC1–SC4, FT1–FT6 và sản phẩm bàn giao |
| [PLAN.md](PLAN.md) | Kế hoạch triển khai và các khoảng trống trước đây; phần trạng thái được cập nhật đến 26/10/2026 |
| [docs/architecture.md](docs/architecture.md) | Luồng dữ liệu, schema, baseline, hợp nhất view và các thay đổi kỹ thuật |
| [docs/decisions.md](docs/decisions.md) | Quyết định triển khai local, k3s trên VM, cấu hình thu gọn và những điểm cần trao đổi với giảng viên |
| [docs/collection_guide.md](docs/collection_guide.md) | Nguồn dữ liệu, backfill, cursor, ngân sách request, lịch thu tiếp và giới hạn dữ liệu |
| [docs/deployment_guide.md](docs/deployment_guide.md), [infra/k3s/README.md](infra/k3s/README.md), [infra/k3s/FAULT_TEST.md](infra/k3s/FAULT_TEST.md) | Các phương án triển khai đầy đủ, demo và kiểm thử trên cụm |
| [van_de_va_giai_phap_ha_tang.md](van_de_va_giai_phap_ha_tang.md) | Phân tích khó khăn tài nguyên và phương án điều chỉnh ban đầu |
| [dashboards/README.md](dashboards/README.md), [notebooks/README.md](notebooks/README.md) | Nội dung dashboard và notebook dự kiến |

Slide môn học có mốc thời gian năm 2024; báo cáo này chỉ sử dụng yêu cầu chuyên môn trong slide, không dùng các mốc đó làm lịch nộp của nhóm hiện tại. `PLAN.md` nhắc `BigData.md`, nhưng file thiết kế gốc thực tế đang có trong repo là `BigData.pdf`.

### 2.2. Quy ước trạng thái

| Trạng thái | Ý nghĩa |
| --- | --- |
| Có triển khai | Đã đọc thấy logic, cấu hình hoặc artifact tương ứng trong repo |
| Có đầu ra dữ liệu thật | Có bảng dữ liệu đã lưu và đã đọc/đếm được khi lập báo cáo |
| Tài liệu ghi đã kiểm chứng | Lịch sử kiểm chứng được ghi trong tài liệu; không mặc định là đã tái chạy trong lần đối chiếu này |
| Chưa đủ bằng chứng nghiệm thu | Còn thiếu log, số đo, ảnh hoặc kết quả chạy đáp ứng điều kiện của thiết kế |

Số liệu bên dưới được đếm trực tiếp từ JSONL gzip, metadata Parquet và một số bảng tổng hợp. Không chạy lại toàn bộ batch, không tạo dữ liệu demo mới và không tác động cụm Kubernetes khi lập báo cáo. Các output đã lưu chứng minh có kết quả hiện hữu, nhưng không thay thế log của một lần chạy toàn trình có thể tái lập.

Không quy đổi số file hoặc số module thành phần trăm hoàn thành chung: một kịch bản phục hồi nhiều node chưa chạy có mức độ quan trọng khác với một file cấu hình đã viết.

## 3. Mục tiêu nghiệp vụ và kiến trúc đã thực hiện

Hệ thống hướng đến năm nhóm câu hỏi: mức hài lòng thay đổi theo thời gian; khía cạnh người chơi phàn nàn; quan hệ giờ chơi và đánh giá; bất thường review/CCU; và game đang tăng trưởng.

Luồng xử lý hiện có:

```text
Steam Reviews / AppDetails / CCU / SteamSpy / dữ liệu lịch sử
                         |
               Python crawlers / replay
                         |
          +--------------+----------------+
          |                               |
    File JSONL gzip                     Kafka
          |                         /             \
          |                   B1 -> raw        S1 / S2 / S3
          |                       |                 |
          +-----------> B2 -> clean                |
                              |                    |
                  B5 -> B3 -> B4 -> B6 -> B7 -> B8 |
                              |                    |
                         curated            realtime / alerts
                              \                    /
                           Elasticsearch + merge_views
                                      |
                    Kibana D1–D4 / SQL / notebook

       Prometheus + Grafana D5: cấu hình giám sát hạ tầng
       Trending -> feedback -> cập nhật danh sách game theo dõi
```

File là đường vào phục vụ phát triển và xử lý bộ dữ liệu đã chia sẻ. Nhánh Kafka/HDFS/Kubernetes phục vụ mục tiêu tích hợp phân tán. Hai đường vào dùng cùng cách tổ chức raw, giúp tái sử dụng các job xử lý.

Điểm chạy chung là [pipeline.py](pipeline.py). Lệnh `batch` chạy **B2 → B5 → B3 → B4 → B6 → B7 → B8**; B1 được gọi riêng khi cần lấy dữ liệu từ Kafka. B5 chạy trước B3 để B3 sử dụng điểm cảm xúc. Khi truyền `--es` cho lệnh batch, bước hợp nhất view được gọi sau chuỗi batch.

## 4. Đối chiếu các yêu cầu chức năng F1–F6

| Mã | Yêu cầu thiết kế | Kết quả hiện có | Đánh giá tiến độ và phần còn lại |
| --- | --- | --- | --- |
| F1 | Crawl định kỳ review, metadata, CCU; Kafka; backfill | Có crawler từng nguồn, crawler local, AppList, chọn target, replay JSONL/Kaggle, backfill lịch sử; raw đã có trên dữ liệu thật | Có triển khai và dữ liệu thật. Cần chứng minh lịch thu liên tục, độ phủ qua nhiều ngày và đường vào Kafka trên cụm |
| F2 | Raw bất biến trên HDFS; clean Parquet theo ngày | B1 ghi raw gzip và lưu offset; B2 tạo review/CCU/game clean; có script và cấu hình HDFS | Có output Parquet local. Chưa đủ bằng chứng raw thật đã được lưu và nhân bản đầy đủ trên HDFS của cụm |
| F3 | Batch tổng hợp, cảm xúc, khía cạnh, xu hướng | B1–B8 đã có; `data/lake/curated` chứa 16 thư mục kết quả | Có triển khai và đầu ra dữ liệu thật. Cần log chạy lại, kiểm tra tính ổn định khi tăng quy mô và lịch chạy hằng ngày trên cụm |
| F4 | Streaming cửa sổ, bất thường, cảnh báo trong vài phút | S1–S3 có watermark, checkpoint, ES sink; S2/S3 có Kafka alert sink và baseline từ B7 | Có triển khai. Tài liệu ghi đã thử file/Kafka; lần đối chiếu này chưa xác nhận độ trễ ≤ 2 phút hoặc phục hồi streaming trên cụm |
| F5 | SQL ad-hoc trên Parquet/Hive; tìm kiếm review trong ES | Có DDL/SQL mẫu, đăng ký bảng khi bật Hive Metastore; `reviews_search` và saved search | Có triển khai. Cấu hình cụm thu gọn chưa triển khai Hive theo tài liệu quyết định; cần minh chứng truy vấn thật và chốt phương án nghiệm thu |
| F6 | Tổng quan, chi tiết game, realtime và cảnh báo | Có mã sinh Kibana D1–D4, NDJSON, Grafana D5; D1 có banner và KPI | Có artifact trực quan hóa. Cần ảnh/demo từng dashboard với dữ liệu thật, xác nhận nguồn realtime và dữ liệu monitoring |

## 5. Tiến độ thu thập và chất lượng dữ liệu

### 5.1. Những chức năng đã xây dựng

- Crawler review có phân trang bằng cursor; crawler CCU, AppDetails, SteamSpy và AppList được tách thành module.
- Có lựa chọn xuất Kafka hoặc JSONL gzip, chia game cho worker, giới hạn tốc độ và xử lý HTTP 429.
- `collect_local.py` hỗ trợ giới hạn request, tiếp tục theo vị trí game và trạng thái thu thập.
- `historical_backfill.py` chuẩn hóa dữ liệu lịch sử, lưu tiến độ theo shard, kiểm tra nguồn/tập target để tiếp tục đúng dữ liệu.
- Có manifest nguồn dữ liệu, revision và SHA-256 cho việc tạo danh sách game và nhập dữ liệu công khai.
- Có replay dữ liệu vào Kafka để backfill hoặc tạo tải kiểm thử.

Minh chứng: [crawlers/collect_local.py](crawlers/collect_local.py), [crawlers/historical_backfill.py](crawlers/historical_backfill.py), [crawlers/build_targets.py](crawlers/build_targets.py), [crawlers/common/jsonl_producer.py](crawlers/common/jsonl_producer.py), [crawlers/common/rate_limiter.py](crawlers/common/rate_limiter.py).

Lưu ý về khả năng phục hồi: CCU crawler gặp 429 thì backoff và chuyển tiếp; game lỗi có thể được thu lại trong lượt sau. Không nên mô tả tất cả crawler là tự retry cho đến thành công ngay trong cùng một lượt.

### 5.2. Số liệu trực tiếp từ dữ liệu hiện có

| Hạng mục | Số liệu kiểm đếm | Diễn giải |
| --- | ---: | --- |
| Target game | 1.000 | Số mục trong `data/target_apps.json`; là danh sách theo dõi |
| Game có review raw | 996 | Chưa phải cả 1.000 game đều có review raw |
| File review raw `.jsonl.gz` | 1.504 | Chỉ tính file dữ liệu gzip đã có |
| Dòng review raw | 1.101.641 | Có thể gồm bản cập nhật và bản ghi lại |
| ID review raw phân biệt | 1.099.441 | Đếm theo `recommendationid`, chưa áp dụng lọc nội dung |
| Dòng vượt quá số ID phân biệt | 2.200 | Chênh lệch do lặp ID; cần dedupe, không cộng thành review mới |
| Review có nguồn lịch sử | 1.002.000 dòng | `source = GianLucaSpagnolo/Steam_Reviews_Dataset_2021`; là số dòng vật lý hiện có |
| Review có nguồn live | 99.641 dòng | `source = steam_live`; chưa khử trùng giữa các nguồn |
| Snapshot metadata | 1.005 dòng / 1.000 game | Tất cả trong ngày 07/10/2026 theo timestamp UTC |
| Snapshot CCU | 1.005 dòng / 1.000 game | Tất cả trong ngày 07/10/2026 theo timestamp UTC |
| Review clean | 1.045.439 dòng | Đọc số dòng từ metadata các file Parquet |
| Review tiếng Anh có sentiment | 380.136 dòng | Bảng `curated/review_sentiment` |
| Dung lượng file trong raw | 160.568.497 byte, khoảng 153,1 MiB | Kích thước file đã nén, không phải dung lượng JSON giải nén |
| Dung lượng file trong lake | 1.014.071.518 byte, khoảng 967,1 MiB | Tổng file hiện có; gồm nhiều bảng dẫn xuất và file phụ, không đại diện số dữ liệu độc lập |

Raw có review từ **21/11/2010 đến 07/10/2026** theo thời gian tạo. Khoảng min–max này không có nghĩa dữ liệu phủ đều hoặc liên tục toàn bộ giai đoạn.

Số dòng raw giảm **56.202** khi so với clean. Không quy toàn bộ chênh lệch đó thành spam: B2 còn xử lý ID lặp, bản cập nhật và bản ghi thiếu trường bắt buộc. Muốn tách chính xác từng nguyên nhân cần bổ sung thống kê trước/sau từng bước.

### 5.3. Khoảng cách so với quy mô thiết kế

Thiết kế gốc nhắm 1.000–2.000 game, 5–10 triệu review lịch sử và khoảng 20–30 GB dữ liệu thô. Hiện danh sách target đã đạt cận dưới 1.000 game, nhưng số review và dữ liệu thời gian thực vẫn chưa đạt toàn bộ quy mô dự kiến. Dung lượng raw gzip không thể so trực tiếp với ước lượng JSON thô chưa nén trong thiết kế.

Các hạn chế cần chủ động nêu:

1. **Thiếu chuỗi CCU nhiều ngày:** 1.005 snapshot trên 1.000 game nghĩa là phần lớn game chỉ có một quan sát. Không đủ đánh giá tăng trưởng tuần hay baseline cùng giờ qua nhiều ngày.
2. **Dữ liệu review lệch mạnh:** riêng appid `292030` chiếm 454.982 review clean, khoảng **43,52%** tổng review clean. Tổng hợp toàn bộ dữ liệu sẽ chịu ảnh hưởng lớn của game này.
3. **Lịch sử và dữ liệu hiện tại không đồng bộ:** metadata/CCU hiện có thuộc ngày 07/10/2026, trong khi review trải dài nhiều năm. Không diễn giải giá/CCU của ngày crawl thành giá/CCU lịch sử của từng review.
4. **Dữ liệu mới nhất chưa đến ngày báo cáo:** bộ dữ liệu hiện hữu có thời điểm sự kiện mới nhất ngày 07/10/2026; cần thu tiếp trước buổi demo nếu muốn thể hiện tính thời gian thực.

## 6. Tiến độ xử lý batch B1–B8

| Job | Đối chiếu code với thiết kế | Minh chứng kết quả hiện có | Phần cần hoàn thiện/kiểm chứng |
| --- | --- | --- | --- |
| B1 — Ingest raw | Đọc Kafka theo offset, ghi JSONL gzip, phân vùng ngày và lưu offset tiếp theo | [b1_ingest_raw.py](processing/batch/b1_ingest_raw.py) | Raw local không tự chứng minh B1 đã chạy trên cụm; cần log Kafka → HDFS và kiểm tra chạy lại |
| B2 — Clean | Parse/ép kiểu; giữ review cập nhật mới nhất; chuẩn hóa văn bản; gắn cờ tiếng Anh/đáng ngờ; làm sạch CCU và metadata | 1.045.439 review, 1.005 CCU, 1.000 game metadata và 1.005 dòng lịch sử metadata | Cần thống kê nguyên nhân loại dòng, kiểm chứng dedupe và tối ưu file nhỏ |
| B3 — Tổng hợp | Theo game/ngày, tuần/tháng; thể loại/nhà phát triển/giá; ngôn ngữ; join sentiment và CCU | 32.738 dòng game/ngày; 9.077 dòng game/kỳ; 2.786 dòng phân khúc; 53.343 dòng ngôn ngữ/ngày | Cần phân tích độ phủ và kiểm tra kết quả khi thêm dữ liệu nhiều ngày |
| B4 — Giờ chơi | 5 nhóm giờ chơi; % tích cực; tiêu cực trước 2 giờ; giờ chơi trung vị và điểm đáng tiền | 4.628 dòng nhóm giờ chơi; 993 dòng hồ sơ game | Chỉ số tiêu cực sớm là đại diện gián tiếp; chưa phải đo retention từ lịch sử hoạt động người dùng |
| B5 — Cảm xúc/khía cạnh | VADER và từ điển cho tiếng Anh; TF-IDF bằng CountVectorizer + IDF trên review tiêu cực; ghi ES | 380.136 dòng sentiment; 12.072 dòng khía cạnh; 25.060 dòng từ khóa | Cần xem mẫu để đánh giá từ điển và cách diễn giải; chưa có đánh giá độ chính xác mô hình |
| B6 — Xu hướng/sự kiện | Cửa sổ 7 ngày so với 7 ngày trước theo lịch; z-score review/CCU; nhận diện bắt đầu giảm giá | 32.738 dòng trending; top 50; 722 dòng so sánh sự kiện | Chưa có nguồn patch/update; tăng trưởng CCU thiếu lịch sử; cần kiểm soát độ phủ trước khi kết luận |
| B7 — Baseline | Review baseline 7 ngày, tính cả giờ không có review; CCU baseline theo game/giờ | 972 dòng review baseline; 1.005 dòng CCU baseline | Có bảng không đồng nghĩa đủ lịch sử: CCU hiện có thể chỉ có một mẫu trong từng game/giờ |
| B8 — Phân cụm | Chuẩn hóa đặc trưng và K-Means; game phải có ít nhất 20 review; mặc định 6 cụm | 990 game; 6 dòng hồ sơ cụm | Cần diễn giải đặc trưng từng cụm và tác động của dữ liệu lệch; không lấy số cụm làm minh chứng chất lượng |

Mã nguồn tương ứng nằm trong [processing/batch](processing/batch). Với B4, công thức đang dùng cho `value_score` là **trung vị tổng giờ chơi `playtime_forever` / giá**, còn nhóm giờ chơi dùng `playtime_at_review`. Hai trường phục vụ hai cách nhìn khác nhau và cần giải thích đúng khi thuyết trình.

B2–B8 có cơ chế ghi đè đầu ra, ES dùng ID ổn định cho từng khóa. B1 ghi raw trước rồi mới lưu offset nên khi sự cố ở giữa hai bước vẫn có thể phát lại dữ liệu; B2 chịu trách nhiệm khử trùng review. Vì vậy chưa nên tuyên bố toàn bộ hệ thống có bảo đảm exactly-once.

### 6.1. Những kết quả phân tích ban đầu có thể trình bày

**Mức hài lòng:** trong bảng tổng hợp đang lưu có 1.007.397 review tích cực trên 1.045.439 review clean, tương ứng khoảng **96,36%**. Đây là tỷ lệ của bộ dữ liệu nhóm đã thu, có lệch mẫu lớn; không phải tỷ lệ hài lòng đại diện cho toàn bộ Steam.

**Khía cạnh phàn nàn:** tổng số lượt nhắc khía cạnh trong review có nhãn tiêu cực như sau:

| Khía cạnh | Lượt nhắc trong review tiêu cực |
| --- | ---: |
| Gameplay | 5.403 |
| Story | 3.104 |
| Price | 1.995 |
| Bug | 1.930 |
| Graphics | 1.586 |
| Performance | 1.554 |
| Server | 1.310 |
| Cheating | 382 |

Một review có thể nhắc nhiều khía cạnh. Cách đếm lấy nhãn tiêu cực `voted_up = false` của review, chưa xác định cảm xúc riêng cho từng khía cạnh. Ví dụ, review tiêu cực có nhắc “story” không tự chứng minh người viết đang chê cốt truyện.

**Giờ chơi và đánh giá:** bảng hiện có ghi nhận Mirror 2: Project X có 33,00% review tiêu cực trước 2 giờ trên tổng 100 review của game trong bảng; Bright Memory là 30,61% trên 98 review. Đây là ví dụ đầu ra của chỉ số `early_negative_ratio`, cần xem quy mô mẫu và nội dung review trước khi kết luận về khả năng giữ chân.

**Trending:** bảng top 50 đã được tạo. Một số giá trị tăng trưởng review rất cao, ví dụ Mechabellum là 9.800%, trong khi tăng trưởng CCU để null. Kết quả này minh họa pipeline đã tính chỉ số, đồng thời cho thấy mẫu số kỳ trước và độ đầy đủ dữ liệu phải được kiểm tra trước khi gọi đó là tăng trưởng thực tế.

**Giảm giá:** có 722 dòng sự kiện; 582 dòng có review ở cả hai phía trước/sau. Code coi snapshot đầu tiên có giảm giá là bắt đầu sự kiện khi chưa có snapshot trước đó; với metadata chỉ trong một ngày, đây có thể là đợt giảm giá đã diễn ra từ trước. Cần xác minh ngày bắt đầu trước khi diễn giải tác động. Mức chênh lệch là quan sát trong dữ liệu, chưa đủ để kết luận giảm giá gây ra thay đổi mức hài lòng. Phần tác động của bản cập nhật vẫn cần nguồn sự kiện bổ sung.

**Phân cụm:** 990 game chia vào 6 cụm với số lượng lần lượt theo ID cụm 0–5 là **121, 80, 255, 36, 68, 430**. Cần đọc hồ sơ đặc trưng trước khi đặt tên hoặc giải thích ý nghĩa các cụm; ID cụm không phải thứ hạng.

## 7. Tiến độ streaming và hợp nhất Lambda

### 7.1. S1 — Chỉ số review

[s1_review_metrics.py](processing/streaming/s1_review_metrics.py) có hai luồng cửa sổ: 60 phút trượt 5 phút và cửa sổ 5 phút. Watermark 10 phút; đầu ra là số review, số tích cực/tiêu cực và tỷ lệ tích cực. Có checkpoint riêng cho mỗi cửa sổ và ghi ES bằng khóa gồm loại chỉ số, appid, độ dài cửa sổ và thời điểm bắt đầu.

**Tiến độ:** logic và sink đã có. Cần minh chứng dữ liệu mới đi qua Kafka, cập nhật ES/D3 và số đo độ trễ thực tế.

### 7.2. S2 — Nghi vấn review bombing

[s2_review_bombing_detector.py](processing/streaming/s2_review_bombing_detector.py) dùng cửa sổ 60 phút trượt 10 phút, watermark 15 phút. Khi có baseline, cảnh báo nếu số review tiêu cực ít nhất 10, vượt trung bình + 3σ và tỷ lệ tích cực giảm hơn 20 điểm phần trăm. Khi thiếu baseline, mặc định dùng ngưỡng tối thiểu 50 review và tỷ lệ tích cực không quá 35%.

Baseline được đọc lại trong từng micro-batch. Khi nguồn là Kafka, cảnh báo được ghi `steam.alerts`; khi bật ES, ghi index `alerts`. Đây là dấu hiệu thống kê để điều tra, chưa phải kết luận chắc chắn có hành vi review bombing.

**Tiến độ:** có logic, nhánh dự phòng, sink và unit test tương ứng. Có checkpoint S2 của lần chạy dữ liệu mẫu trong workspace; checkpoint đó không đủ chứng minh đã phát cảnh báo đúng hoặc đạt SLA trên dữ liệu thật.

### 7.3. S3 — Đột biến CCU

[s3_ccu_spike_detector.py](processing/streaming/s3_ccu_spike_detector.py) tổng hợp cửa sổ 10 phút và so với baseline cùng giờ. Mặc định cảnh báo tăng/giảm trên 50%, bỏ qua baseline dưới 100. Có đầu ra realtime, loại `CCU_SPIKE`/`CCU_DROP`, cảnh báo ES/Kafka.

**Tiến độ:** có triển khai; dữ liệu thật hiện chưa đủ lịch sử CCU nhiều ngày để đánh giá khả năng phát hiện. Cần tách replay sự kiện giả có kiểm soát khỏi kết luận bất thường trong thực tế.

### 7.4. Hợp nhất view và vòng phản hồi

[merge_views.py](processing/serving/merge_views.py) xác định cutoff, xóa realtime có `window_end <= cutoff`, rồi lưu trạng thái vào index `serving_state`. [update_targets_from_trending.py](crawlers/update_targets_from_trending.py) bổ sung game trending vào danh sách theo dõi và giới hạn kích thước danh sách; đã có unit test.

Hai điểm cần kiểm tra tiếp trước nghiệm thu:

- Cutoff mặc định là ngày mới nhất trong batch cộng một ngày. Nếu batch chạy khi ngày đó chưa đủ dữ liệu, cần xác nhận cutoff không làm xóa quá mức phần realtime còn cần thiết.
- Dashboard hiện truy vấn riêng index batch và realtime; chưa thấy mã đọc `serving_state` trong `setup_kibana.py`. Cần demo cách hiển thị dữ liệu quanh cutoff trước khi tuyên bố đã hoàn thiện trải nghiệm hợp nhất liên tục.

## 8. Tiến độ lưu trữ, truy vấn và dashboard

### 8.1. Lưu trữ và truy vấn

Mapping chuẩn ở [storage/elasticsearch/indices_mapping.json](storage/elasticsearch/indices_mapping.json) có **15 index**, mở rộng từ 5 index lõi của thiết kế. Các index bổ sung phục vụ rollup, ngôn ngữ, nhóm giờ chơi, retention, từ khóa, trending, tác động sự kiện, phân cụm và trạng thái serving.

[es_writer.py](processing/common/es_writer.py) bulk theo partition, đặt ID từ khóa nghiệp vụ và báo lỗi ghi thay vì bỏ qua. [spark_utils.py](processing/common/spark_utils.py) dùng được đường dẫn local/HDFS và đăng ký bảng khi có `HIVE_METASTORE_URI`. Có [DDL](storage/hive/create_tables.sql) và [SQL ad-hoc mẫu](storage/hive/adhoc_queries.sql).

**Còn lại:** lưu ví dụ truy vấn thật, kết quả full-text, số document từng index và đối soát với Parquet. Cấu hình index có sẵn chưa chứng minh ES đang chứa đủ dữ liệu hoặc đang phục vụ bình thường.

### 8.2. Đối chiếu D1–D5

| Dashboard | Đã có trong code/artifact | Khác biệt hoặc phần cần bổ sung |
| --- | --- | --- |
| D1 — Tổng quan | Xu hướng review, tỷ lệ tích cực theo thể loại, top CCU, ngôn ngữ, heatmap giá; banner và 4 KPI | KPI game là cardinality ước lượng; heatmap giá dùng tổng hợp cả kỳ. Cần ảnh render trên dữ liệu thật |
| D2 — Chi tiết game | Tỷ lệ tích cực theo tuần, CCU theo ngày, nhóm giờ chơi, khía cạnh, word cloud và tìm kiếm review; lọc `appid:<id>` | Chưa có annotation cập nhật ngay trên timeline; CCU chưa chi tiết theo giờ như mục tiêu thiết kế; tác động giảm giá đặt ở D4 |
| D3 — Realtime | Review từ cửa sổ 5 phút, tỷ lệ tích cực cửa sổ 60 phút và top CCU | Biểu đồ review đang gom theo giờ; top CCU dùng max trong khoảng lọc, cần kiểm tra trước khi mô tả là giá trị mới nhất |
| D4 — Cảnh báo và xu hướng | Biểu đồ/bảng cảnh báo, bảng trending và tác động giảm giá | Cần dữ liệu cảnh báo thật hoặc replay có nhãn, kiểm tra kết nối và độ trễ |
| D5 — Hệ thống | Có JSON Grafana và cấu hình monitoring cho Kafka, tài nguyên và HDFS | Theo `docs/decisions.md`, monitoring chưa triển khai trong phương án thu gọn; cần kiểm tra exporter, datasource và panel thực sự có số liệu |

Minh chứng: [setup_kibana.py](dashboards/setup_kibana.py), [overview_style.py](dashboards/overview_style.py), [steam_dashboards.ndjson](dashboards/kibana/steam_dashboards.ndjson), [steam_overview.ndjson](dashboards/kibana/steam_overview.ndjson), [d5_system.json](dashboards/grafana/d5_system.json).

Riêng D1 đã được chăm chút phần trình bày: bố cục lưới 48 cột, banner, KPI tổng review/tỷ lệ tích cực/số game/CCU đỉnh, bảng màu và tooltip. Có chế độ `--overview-only` để cập nhật riêng D1. Đây là phần cải thiện khả năng demo; vẫn cần kiểm tra trực quan trên Kibana trước buổi báo cáo.

## 9. Tiến độ hạ tầng Kubernetes và các điều chỉnh thiết kế

### 9.1. Những gì đã chuẩn bị

- Manifest Kafka Strimzi/KRaft, HDFS, Elasticsearch ECK/Kibana, Hive/PostgreSQL và monitoring.
- Batch CronJob chạy B1 rồi B2–B8; Deployment riêng cho S1–S3 với chiến lược `Recreate`.
- Cấu hình Spark dynamic allocation tối thiểu 2, tối đa 6 executor ở bộ manifest đầy đủ.
- CronJob crawler CCU, reviews, AppDetails, SteamSpy và AppList.
- Script cài cụm, triển khai, demo, kiểm thử, tạo thư mục và khởi tạo index.
- Bộ `k8s/demo` và `k8s/fault-test` có namespace, PVC và cấu hình riêng; có observer kiểm tra dữ liệu ACK của Kafka và dữ liệu đọc từ HDFS.

Minh chứng: [k8s/spark/steam-jobs.yaml](k8s/spark/steam-jobs.yaml), [k8s/common/pipeline-config.yaml](k8s/common/pipeline-config.yaml), [k8s/crawlers/crawler-cronjobs.yaml](k8s/crawlers/crawler-cronjobs.yaml), [scripts/deploy_all.sh](scripts/deploy_all.sh), [infra/k3s/fault_watch.py](infra/k3s/fault_watch.py).

Lịch batch trong manifest là `01:30` với chú thích UTC, tương ứng `08:30` giờ Việt Nam nếu controller dùng UTC. Chưa có `spec.timeZone` trong manifest đó; cần xác nhận múi giờ scheduler khi triển khai.

### 9.2. Phân biệt thiết kế đầy đủ và môi trường thu gọn

| Thành phần | Thiết kế gốc / bộ cấu hình đầy đủ | Phương án thu gọn trong tài liệu và `k8s/fault-test` | Hệ quả khi báo cáo |
| --- | --- | --- | --- |
| Kubernetes | Môi trường nhiều node, yêu cầu ≥ 3 worker | Quyết định hướng tới 4 VM: 1 server + 3 agent; có hướng dẫn demo nhỏ hơn | Cần log `nodes` và vị trí pod; chưa có bằng chứng cụm thật trong lần đối chiếu này |
| Kafka | 3 broker, RF=3, min ISR=2; topic 6/3/3/1 partition | Giữ 3 broker/RF=3/min ISR=2; hướng dẫn fault-test dùng topic 3 partition | Cần ghi rõ topology và số partition thực tế của từng lần test |
| HDFS | 1 NameNode + 3 DataNode, replication 3 | 1 NameNode + 2 DataNode, replication 2 | Có thể thử mất một DataNode nếu đã có đủ bản sao; chưa HA NameNode |
| Elasticsearch | 3 node, 1 replica | 1 node, 0 replica | Chưa đáp ứng FT4/SC4 trong cấu hình thu gọn |
| Hive Metastore | Có Hive + PostgreSQL | Chưa triển khai theo `docs/decisions.md`; dùng Spark SQL trực tiếp Parquet | Cần chốt với giảng viên có chấp nhận điều chỉnh không |
| Monitoring | Prometheus + Grafana D5 | Tài liệu quyết định ghi chưa triển khai; tạm theo dõi bằng công cụ cụm | Không tuyên bố D5 đã vận hành đầy đủ |
| Spark job | Thiết kế ban đầu dùng Spark Operator | Code hiện dùng CronJob/Deployment, driver client mode, executor pod | Khác cách điều phối, vẫn cần chứng minh Spark thực sự chạy phân tán |

`van_de_va_giai_phap_ha_tang.md` từng đề xuất 200–300 game và ghép 5 máy cá nhân; `docs/decisions.md` cập nhật sau chốt mục tiêu 1.000 game và phương án VM cloud theo buổi. Báo cáo dùng quyết định cập nhật cùng dữ liệu hiện có; các phương án cũ được giữ làm lịch sử, không cộng thành triển khai đã hoàn thành.

Hiện vẫn có một NameNode và một k3s server trong phương án thu gọn. Vì vậy không thể khẳng định “mất bất kỳ node nào hệ thống vẫn liên tục phục vụ”. Restart một pod và tắt một VM là hai mức lỗi khác nhau; local-path PVC cũng cần được xem xét khi mất hẳn node chứa volume.

## 10. Tiến độ kiểm thử và bằng chứng hiện có

### 10.1. Kiểm thử chạy trong lần lập báo cáo

Đã sử dụng Python của môi trường Conda `steam-insights` để chạy:

```bash
HIVE_METASTORE_URI='' SPARK_PACKAGES='' SPARK_LOCAL_IP=127.0.0.1 \
  /home/nqkie/miniconda3/envs/steam-insights/bin/python -m pytest \
  -q -p no:cacheprovider tests/unit tests/infrastructure
```

**Kết quả: 11 ca đạt, 4 ca lỗi ở bước khởi tạo Spark.** Bốn ca là luật S2 có baseline, S2 dự phòng, S3 tăng/giảm CCU và chia nhóm giờ chơi B4. Java gateway bị chặn mở socket với `java.net.SocketException: Operation not permitted` trong môi trường thực thi lần này; các assertion của bốn ca đó chưa chạy được. Đây không phải kết luận bốn logic trên bị sai.

11 ca đạt bao gồm 7 ca unit không cần Spark và 4 ca infrastructure: tiền xử lý text, khía cạnh, chia worker, target/replay, feedback, observer phát hiện bản ghi ACK bị thiếu, observer chấp nhận dữ liệu đầy đủ, tham số submit executor và entrypoint. Các bài infrastructure dùng mock hoặc kiểm tra lệnh, không thay cho kiểm thử lỗi trên cụm thật.

`PLAN.md` ghi unit 11/11 và một số thử nghiệm rút gọn đã đạt vào 26/10/2026. Đó là thông tin lịch sử; kết quả chạy lại ngày 11/11/2026 được ghi riêng ở trên, tránh nhầm hai lần kiểm chứng.

### 10.2. Scalability SC1–SC4

| Kịch bản | Công cụ đã có | Trạng thái minh chứng | Việc cần làm |
| --- | --- | --- | --- |
| SC1 — Tăng executor batch | `tests/scalability/sc1_sc2_batch_benchmark.py`; benchmark shuffle riêng trong `infra/k3s/benchmark.py` | Có code; chưa có CSV trong workspace | Chạy cùng dữ liệu với 1/2/4/6 executor, xác nhận executor trên node, ghi thời gian và speedup |
| SC2 — Tăng dữ liệu | Script SC1/SC2 hỗ trợ hệ số 1×/2×/5×/10× | Có code; chưa có CSV | Cố định tài nguyên, tăng dữ liệu, ghi số dòng/dung lượng và thời gian; phân biệt dữ liệu thật với dữ liệu sinh |
| SC3 — Tải streaming | `tests/scalability/sc3_streaming_load_test.py`, progress logger | Có code đo throughput, batch duration và offset lag | Chạy các mức tải/partition/executor; bổ sung đo end-to-end từ lúc nhập sự kiện đến khi thấy view |
| SC4 — Serving | `tests/scalability/sc4_es_query_load.py` | Có code đo QPS, p50/p95/p99 và chờ green | Cần ES nhiều node; chạy cùng truy vấn với 2 rồi 3 node, lưu số liệu phân bổ lại shard |

Trong chế độ local của SC1, tham số “executor” là số core `local[N]`, không phải N executor phân tán. Benchmark shuffle trong `infra/k3s` chứng minh năng lực xử lý phân tán của một workload riêng; không thay thế kết quả B2–B5.

Thời gian micro-batch và consumer lag là các chỉ số cần thiết nhưng chưa tự chứng minh SLA ≤ 2 phút. Với review lịch sử replay, không lấy chênh lệch từ `timestamp_created` nhiều năm trước đến thời điểm hiện tại làm độ trễ vận hành của pipeline.

### 10.3. Fault-tolerance FT1–FT6

| Kịch bản | Chuẩn bị trong repo | Khoảng trống để nghiệm thu |
| --- | --- | --- |
| FT1 — Mất Kafka broker | Chaos script và observer kiểm tra record đã ACK | Cần thực thi khi producer đang chạy, lưu ISR/leader, số ACK/ID nhận lại và thời gian phục hồi |
| FT2 — Mất DataNode | Kịch bản đọc/ghi, probe và kiểm tra bản sao trong hướng dẫn k3s | Cần xác nhận đủ bản sao trước khi gây lỗi; script tổng quát đòi 3 DataNode, không dùng nguyên điều kiện đó cho cụm 2 DataNode |
| FT3 — Mất executor/driver | Có xóa executor, restart driver và checkpoint | Cần log task retry, offset/checkpoint và đối soát ID; chỉ thấy số document tăng chưa đủ chứng minh không mất/trùng |
| FT4 — Mất node ES | Có chaos script | Cần nhiều node và replica; ES một node/0 replica chưa kiểm chứng được yêu cầu này |
| FT5 — Mất worker | Có drain node và hướng dẫn dừng agent/tắt VM | Cần lượt tắt VM thực sự nếu nghiệm thu theo thiết kế; drain có kiểm soát khác với mất máy đột ngột |
| FT6 — API lỗi 429 | Có mock API và `ft6_crawler_rate_limit.py` | Tài liệu ghi đã thử; chưa tái chạy trong lần lập báo cáo. Cần lưu output backoff và thu đủ game ở lượt tiếp theo |

Không chạy các kịch bản gây lỗi khi lập tài liệu này. [infra/k3s/FAULT_TEST.md](infra/k3s/FAULT_TEST.md) cũng ghi repo chưa có kết quả kiểm thử trên cụm thật. Khi thực hiện, cần lưu CSV, log trước/trong/sau lỗi và video; smoke test thành công chỉ xác nhận luồng cơ bản.

## 11. Tiến độ notebook và sản phẩm bàn giao

| Sản phẩm | Hiện trạng | Việc còn lại |
| --- | --- | --- |
| Mã nguồn các tầng | Đã có crawler, batch, streaming, serving, dashboard, hạ tầng và công cụ kiểm thử | Cố định phiên bản, build image đúng code và kiểm tra triển khai lại |
| Notebook EDA | Có `01_eda_raw_reviews.ipynb`, 6 code cell | Chưa lưu execution count/output; cần chạy trên dữ liệu thật và lưu hình/kết luận |
| Notebook cảm xúc/khía cạnh | Có `02_sentiment_aspect_exploration.ipynb`, 7 code cell | Chưa lưu execution count/output; cần diễn giải kết quả và kiểm tra mẫu |
| Notebook SC/FT | Có `03_scalability_fault_tolerance_analysis.ipynb`, 6 code cell | Chưa lưu execution count/output; cần CSV thực nghiệm làm đầu vào |
| Hướng dẫn kiến trúc và vận hành | Có README, kiến trúc, setup local, collection, deployment, k3s và fault test | Đồng bộ những chỗ khác nhau giữa thiết kế đầy đủ và môi trường thu gọn |
| Báo cáo tiến độ | File hiện tại | Cập nhật sau mỗi đợt tích hợp/kiểm thử mới |
| Báo cáo cuối kỳ, slide và video | Chưa thấy bộ sản phẩm cuối kỳ tương ứng trong repo | Chuẩn bị sau khi có minh chứng phân tích và thử nghiệm |

## 12. Các khó khăn và hướng xử lý tiếp theo

| Khó khăn thực tế | Ảnh hưởng đến kết quả | Hướng xử lý |
| --- | --- | --- |
| RAM/CPU hạn chế khi chạy toàn bộ JVM service | Khó bật đồng thời Kafka, HDFS, Spark, ES, Kibana và monitoring | Phát triển local theo từng nhóm dịch vụ; tích hợp trên cụm VM theo quyết định đã ghi; kiểm tra tài nguyên từng node |
| CCU và metadata thiếu lịch sử | Baseline CCU yếu; chưa tính được tăng trưởng tuần; khó phân tích sự kiện | Thu định kỳ tối thiểu 14 ngày cho so sánh 7 ngày với 7 ngày trước; theo dõi số mẫu từng game/giờ |
| Lịch thu thực tế nhẹ hơn thiết kế ban đầu | Giảm độ phân giải thời gian và tốc độ phát hiện | Báo cáo rõ lịch thật; điều chỉnh worker/ngân sách request hoặc xin thống nhất lại yêu cầu |
| Review lịch sử tập trung ở một số game | Chỉ số chung và NLP bị chi phối bởi game lớn | Báo cáo theo game/thể loại, đặt ngưỡng mẫu, kiểm tra phân bố và mở rộng backfill có kiểm soát |
| Quá nhiều file Parquet nhỏ | Tăng chi phí liệt kê/đọc file, chuyển lên HDFS và benchmark | Hiện `clean/reviews` có 61.524 file và `review_sentiment` có 54.613 file; cần đo kích thước file và compact/repartition thích hợp |
| Cấu hình đầy đủ và thu gọn khác nhau | Script test có thể dùng sai namespace, số replica hoặc tiêu chí PASS | Chốt một cấu hình cho mỗi đợt test; lưu topology, image tag và tham số cùng kết quả |
| Bằng chứng chạy hệ thống chưa được gom lại | Khó chứng minh tiến độ dù đã có code/output | Lưu log, CSV, ảnh dashboard và video vào bộ artifact có ngày chạy và commit |

## 13. Kế hoạch hoàn thành phần còn lại

Các mốc dưới đây là đề xuất thứ tự triển khai sau báo cáo tiến độ, chưa phải lịch đã cam kết hoặc công việc đã thực hiện.

| Ưu tiên | Công việc | Đầu ra cụ thể | Điều kiện hoàn thành |
| --- | --- | --- | --- |
| P0 | Chốt phạm vi nghiệm thu với giảng viên | Danh sách thống nhất về k8s, Hive, replication, ES và quy mô | Có yêu cầu rõ ràng cho cấu hình cuối cùng |
| P0 | Thu tiếp CCU/metadata/review và kiểm tra độ phủ | Bảng số mẫu theo ngày/game/giờ; manifest dữ liệu mới | Có lịch sử đủ cho các cửa sổ phân tích; tách game thiếu mẫu |
| P0 | Xác nhận batch dữ liệu thật tái lập được | Log B2–B8, thống kê trước/sau và kiểm tra chạy lại | Chạy thành công trên nguồn đã cố định; đối soát số dòng/ID và output |
| P0 | Tích hợp Kafka → HDFS → batch/streaming → ES trên cụm | Log job, danh sách node/pod, count index và ảnh D1–D4 | Đường đi đầy đủ chạy trên topology đã chốt |
| P1 | Kiểm tra merge và D3 | Ca kiểm thử quanh cutoff, dữ liệu mới và phục hồi checkpoint | View không bị xóa quá mức; chỉ số realtime có ý nghĩa đúng như nhãn hiển thị |
| P1 | Chạy SC1–SC4 | CSV, biểu đồ speedup, throughput, lag, latency và ES query | Cấu hình/khối lượng được ghi rõ; so sánh cùng điều kiện, không bỏ lần chạy lỗi |
| P1 | Chạy FT1–FT6 theo khả năng cụm | Log gây lỗi/phục hồi, đối soát dữ liệu, CSV và video | Chứng minh theo từng loại lỗi; phần không đáp ứng phải nêu rõ giới hạn |
| P1 | Chạy notebook và hoàn thiện phân tích | Notebook có output; hình và câu trả lời cho 5 câu hỏi nghiệp vụ | Mỗi nhận xét có số liệu, khoảng thời gian, quy mô mẫu và hạn chế |
| P2 | Hoàn thiện bàn giao | Báo cáo cuối kỳ, slide, video, hướng dẫn dựng lại | Người khác có thể chạy lại theo tài liệu và nhận được kết quả tương ứng |

Gợi ý phân công theo vai trò: thu thập/chất lượng dữ liệu; batch/phân tích; streaming/serving; hạ tầng/kiểm thử phân tán; dashboard/tổng hợp báo cáo. Đây là phương án để nhóm tự gán tên, không phải xác nhận đóng góp của từng thành viên.

## 14. Kịch bản trình bày và demo với giảng viên

### 14.1. Trình tự đề xuất trong 8–10 phút

1. **Mục tiêu và kiến trúc — 1 phút:** nêu 5 câu hỏi thực tế, vì sao cần batch và streaming, chỉ luồng dữ liệu.
2. **Dữ liệu đã có — 1–2 phút:** trình bày target 1.000 game, hơn 1,1 triệu dòng review raw, hơn 1,04 triệu review clean; phân biệt raw, ID phân biệt và output đã làm sạch.
3. **Kết quả batch và dashboard — 2–3 phút:** mở D1, lọc một game ở D2, giải thích giờ chơi/khía cạnh/từ khóa; chỉ ra một nhận xét có số liệu và hạn chế mẫu.
4. **Streaming — 1–2 phút:** giải thích baseline S2/S3. Nếu có môi trường đã chuẩn bị, replay sự kiện có kiểm soát để minh họa D3/D4; ghi rõ đây là dữ liệu thử.
5. **Khoảng trống và kế hoạch — 2 phút:** nêu trạng thái cụm, SC/FT, độ trễ, dữ liệu CCU; chốt thứ tự việc cần hoàn thành và các điều chỉnh cần giảng viên xác nhận.

### 14.2. Những minh chứng nên chuẩn bị trước buổi báo cáo

- Ảnh hoặc bản ghi D1–D4 với time filter phù hợp ngày dữ liệu; dữ liệu cũ không hiển thị nếu dùng mặc định “24 giờ gần nhất”.
- Một ví dụ review raw → clean → sentiment/aspect → bảng tổng hợp, dùng cùng appid/ID để giải thích được nguồn gốc chỉ số.
- Log của một lần chạy batch và bảng tổng hợp số dòng, nguồn dữ liệu, ngày sự kiện.
- Nếu trình diễn cụm: `kubectl get nodes` và `kubectl get pods -A -o wide`, cùng vị trí driver/executor và các replica.
- Nếu chưa có SC/FT, trình bày bộ công cụ và kế hoạch chạy, ghi đúng là “đã chuẩn bị kịch bản”.

### 14.3. Đoạn phát biểu có thể dùng trực tiếp

> Thưa cô/thầy, đến thời điểm báo cáo, nhóm đã xây dựng các thành phần chính của hệ thống Steam Player Insights theo kiến trúc Lambda, gồm thu thập, lưu trữ, xử lý batch, streaming, truy vấn và dashboard. Bộ dữ liệu hiện có hơn 1,1 triệu dòng review raw; các bảng kết quả đã lưu có khoảng 1,045 triệu review sau làm sạch, 380 nghìn review tiếng Anh được phân tích cảm xúc và 990 game được phân cụm. Nhóm đã có kết quả dữ liệu thật để trình bày, đồng thời đã viết manifest Kubernetes và công cụ kiểm thử. Phần nhóm cần hoàn thành tiếp là tích hợp toàn bộ luồng trên cụm nhiều node, thu thêm lịch sử CCU và đo định lượng khả năng mở rộng, phục hồi khi lỗi, độ trễ streaming. Nhóm sẽ báo cáo riêng phần đã có đầu ra và phần mới chuẩn bị công cụ để phản ánh đúng tiến độ.

## 15. Các điểm cần trao đổi với giảng viên

1. Trong cấu hình thu gọn, dùng Spark SQL đọc Parquet trực tiếp thay Hive Metastore có đáp ứng F5 không?
2. Replication HDFS 2 thay 3 và ES một node/0 replica có được chấp nhận cho báo cáo tiến độ; cấu hình nào bắt buộc phải nâng trước nghiệm thu FT4/SC4?
3. Yêu cầu “No docker” áp dụng thế nào với image build, containerd của k3s và Compose dùng để thử local; minh chứng cuối kỳ sẽ dùng Kubernetes/cloud?
4. Quy mô và độ phủ tối thiểu cần đạt khi thực tế có khoảng 1 triệu review clean nhưng lịch sử CCU còn ngắn; có cần đạt đúng ước lượng 20–30 GB trong thiết kế không?
5. Đối với FT5, cần tắt VM thực sự hay chấp nhận dừng agent/drain trong một số lượt thử; tiêu chí dữ liệu và thời gian phục hồi cụ thể là gì?

Những câu hỏi này xuất phát từ khoảng cách giữa thiết kế gốc và quyết định thu gọn đã ghi trong repo. Chưa có nội dung xác nhận của giảng viên trong các tài liệu được đối chiếu.
