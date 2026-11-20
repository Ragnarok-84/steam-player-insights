**STEAM PLAYER INSIGHTS**

**Mô tả dự án và kiến trúc đề xuất**

Hệ thống phân tích trải nghiệm người chơi đối với game trên Steam**Môn học:** Lưu trữ và xử lý dữ liệu lớn (Direction 1: xây dựng và cài đặt kiến trúc dữ liệu lớn) 

# 1\. Dự án này làm gì?

Steam là nền tảng bán game PC lớn nhất thế giới. Mỗi ngày người chơi để lại hàng trăm nghìn **review** (đánh giá kèm nhận xét), và Steam công bố **CCU** (concurrent users: số người đang chơi một game tại một thời điểm). Hai nguồn này phản ánh khá trực tiếp trải nghiệm của người chơi.

Dự án xây dựng một hệ thống dữ liệu lớn hoàn chỉnh: tự động thu thập dữ liệu từ Steam, lưu trữ theo nhiều tầng, xử lý định kỳ (batch) và xử lý liên tục gần thời gian thực (streaming), rồi hiển thị kết quả trên dashboard. Môn học yêu cầu **trọng tâm là kiến trúc dữ liệu lớn**, không phải mô hình học máy, nên các phương pháp phân tích được giữ ở mức đơn giản (từ điển cảm xúc, thống kê, K-Means).

## 1.1. Năm câu hỏi nghiệp vụ hệ thống cần trả lời

| \# | Câu hỏi | Thành phần trả lời |
| :---- | :---- | :---- |
| 1 | Mức độ hài lòng của người chơi với một game thay đổi thế nào theo thời gian? | B3 (thống kê ngày/tuần/tháng), B5 (cảm xúc), dashboard D1, D2 |
| 2 | Người chơi phàn nàn về khía cạnh nào (đồ hoạ, giá, lỗi, server...)? | B5 (từ điển khía cạnh, TF-IDF) |
| 3 | Giờ chơi liên quan thế nào tới đánh giá? Game có "đáng tiền" không? | B4 (nhóm giờ chơi, tỷ lệ chê sớm, điểm đáng tiền) |
| 4 | Có bất thường không (review bombing, số người chơi tăng/giảm đột ngột)? | S2, S3 (streaming), B7 (baseline), D3, D4 |
| 5 | Game nào đang tăng trưởng? Giảm giá có tác động gì? | B6 (trending, tác động giảm giá), vòng phản hồi |

 

## 1.2. Yêu cầu của môn học

•      Có đủ các thành phần: **thu thập, lưu trữ, xử lý, truy vấn, trực quan hoá**.

•      Chạy trên **Kubernetes hoặc cloud**, không dùng Docker để thay cho Kubernetes.

•      **Mô phỏng môi trường phân tán** (nhiều máy/node).

•      **Kiểm thử khả năng mở rộng (scalability) và chịu lỗi (fault-tolerance)**.

•      Dashboard hữu ích, có phân tích tìm ra thông tin ẩn trong dữ liệu.

| Bối cảnh nhóm Nhóm 5 thành viên, thời gian 8 tuần. Mỗi người có một PC 16 GB RAM, nhưng sau khi trừ Windows và trình duyệt thì WSL còn khoảng 8 GB. Vì vậy nhóm chọn: phát triển và demo toàn trình trên một máy; phần phân tán và kiểm thử chạy trên cụm 4 máy ảo Google Cloud, chỉ bật theo buổi. |
| :---- |

 

# 2\. Dữ liệu

## 2.1. Nguyên tắc: chỉ dùng API chính thức của Steam

**Chỉ thu thập trực tiếp từ API Steam**. Không dùng dataset Hugging Face, Kaggle hay SteamSpy, kể cả để chọn danh sách game.

Lý do: bộ dữ liệu cũ (khoảng 1,1 triệu review) trộn từ nhiều nguồn mà không ghi rõ phần nào từ đâu. Khi trộn, các chỉ số theo thời gian (số review mỗi ngày, xu hướng) bị sai lệch và không giải thích được. Dữ liệu chỉ từ Steam nhỏ hơn nhưng rõ nguồn gốc, và có thể mở rộng dần bằng cách thu liên tục.

## 2.2. Các loại dữ liệu thu được

| Nguồn (tham số \--sources) | Endpoint Steam | Lấy được gì | Số request |
| :---- | :---- | :---- | :---- |
| chọn game | IStoreService/GetAppList (cần API key) | Danh sách toàn bộ game trên Steam (catalog) | \~5 / lần |
| chọn game | ISteamChartsService/GetMostPlayedGames | Top \~100 game theo số người chơi, kèm hạng tuần trước | 1 |
| store | IStoreBrowseService/GetItems | Tổng số review, % tích cực, ngày phát hành, early access, tag kèm trọng số, Steam Deck | 1 / 200 game |
| store | IStoreService/GetTagList | Tên của các tag | 1 |
| metadata | store.steampowered.com/api/appdetails | Giá, giảm giá, thể loại, nhà phát triển/phát hành, nền tảng, Metacritic, DLC, achievement, số ngôn ngữ | 1 / game |
| histogram | appreviewhistogram/{appid} | Số review tích cực/tiêu cực theo ngày (30 ngày gần nhất) và theo tháng (cả vòng đời) | 1 / game |
| ccu | ISteamUserStats/GetNumberOfCurrentPlayers | Số người đang chơi ngay lúc gọi | 1 / game |
| reviews | IUserReviewsService/GetAppReviews | Nội dung review, thích/không thích, thời gian, giờ chơi, vote hữu ích, refund, Steam Deck... | ≤ 10 trang / game / lượt |

 

| Hai giới hạn quan trọng của Steam •      Tốc độ: khoảng 1 request mỗi 1,5 giây. 1.000 game mất 4–6 giờ cho lượt crawl đầu. •      Không có API lịch sử CCU: Steam chỉ cho biết số người chơi ở thời điểm gọi. Muốn có lịch sử thì phải tự gọi định kỳ (khoảng 10 phút/lần). Máy tắt thì mất dữ liệu khoảng đó, không lấy lại được. |
| :---- |

 

## 2.3. Cách chọn game theo dõi

Lần thử lấy ngẫu nhiên 30 game trên toàn catalog cho kết quả rất kém: 13/30 game không có review, 11/30 game chưa phát hành, và 5 game chiếm 89,7% số review. Khảo sát 400 game cho thấy khoảng 70% catalog có dưới 20 review và 29% chưa phát hành. Vì vậy danh sách được chọn có phương pháp (crawlers/build\_targets.py), gồm hai nhóm:

•      **Nhóm \`top\_ccu\`** (20 game): các game đứng đầu bảng most-played chính thức của Steam. Nhóm này luôn có hoạt động, dùng cho phần realtime và CCU.

•      **Nhóm \`stratified\`** (80 game): bỏ game chưa phát hành và game dưới 20 review, xáo catalog bằng một seed cố định, rồi chia đều cho 4 tầng số review: 20–99, 100–999, 1k–9,9k, 10k trở lên (20 game mỗi tầng).

Danh sách **chỉ được mở rộng, không xáo lại**. Khi chạy lại với \--limit lớn hơn, game cũ giữ nguyên, game mới không trùng game cũ, và mỗi đợt bổ sung được ghi vào manifest (seed, ngưỡng, chỉ tiêu từng tầng). Cần lưu ý trong báo cáo: mẫu này **không đại diện cho toàn bộ Steam** vì đã loại có chủ đích game ít review và game chưa phát hành.

## 2.4. Quy mô

| Giai đoạn | Quy mô |
| :---- | :---- |
| Thiết kế gốc | 1.000–2.000 game, hàng triệu review, 20–30 GB dữ liệu thô |
| Demo báo cáo tiến độ (hiện tại) | 100 game cố định, 92.019 review, raw 18,3 MiB, lake 166 MiB |
| Sau báo cáo tiến độ | Mở rộng lên khoảng 1.000 game (tối đa 2.000), thu liên tục ≥ 14 ngày |
| Gần ngày bảo vệ | Bật vòng phản hồi: mỗi ngày thêm tối đa 5 game đang lên |

 

# 3\. Kiến trúc tổng quan: Lambda

## 3.1. Kiến trúc Lambda là gì?

Kiến trúc Lambda chia việc xử lý dữ liệu thành hai nhánh chạy song song, rồi gộp kết quả ở một tầng phục vụ chung:

| Nhánh | Cách chạy | Ưu điểm | Nhược điểm |
| :---- | :---- | :---- | :---- |
| **Batch layer** (nhánh lô) | Mỗi ngày tính lại toàn bộ từ dữ liệu gốc | Chính xác, đầy đủ, chạy lại không sợ sai | Chậm: kết quả có sau vài giờ |
| **Speed layer** (nhánh tốc độ) | Xử lý liên tục từng nhóm nhỏ sự kiện vừa đến | Nhanh: kết quả sau vài chục giây | Chỉ thấy dữ liệu gần đây, có thể thiếu chính xác |
| **Serving layer** (tầng phục vụ) | Gộp kết quả hai nhánh | Người dùng thấy cả số liệu lịch sử chính xác lẫn số liệu mới nhất | Phải xử lý chỗ giao nhau giữa hai nhánh |

 

Ví dụ dễ hiểu: batch giống sổ kế toán chốt cuối ngày, chính xác nhưng phải chờ; speed giống bảng tin cập nhật liên tục trong ngày. Đến khi sổ ngày hôm qua đã chốt, bảng tin của hôm qua bị bỏ đi và chỉ giữ phần hôm nay.

## 

## 

## 3.2. Sơ đồ luồng dữ liệu

![][image1]  
Ngoài ra còn **vòng phản hồi**: crawlers/update\_targets\_from\_trending.py thêm vào danh sách theo dõi những game đang lên trên bảng most-played (mới vào top hoặc tăng hạng so với tuần trước). Lượt crawl tiếp theo tự lấy dữ liệu cho game mới. Tín hiệu lấy từ Steam chứ không từ B6, vì B6 chỉ xếp hạng được game đã có trong danh sách.

# 4\. Chi tiết từng tầng

## 4.1. Tầng thu thập (crawler)

•      Viết bằng Python, nằm trong thư mục crawlers/. Đầu ra là file JSONL nén gzip (khi phát triển) hoặc Kafka (khi chạy streaming).

•      **Crawl review tăng dần, không trùng** (crawlers/common/review\_state.py). Mỗi lượt, trong hạn mức trang của game: (1) lấy review mới hơn review mới nhất của lượt trước, dừng khi gặp review đã có; (2) nếu lượt trước bị ngắt giữa chừng thì lấy nốt phần còn thiếu; (3) dùng số trang còn lại để lấy tiếp review cũ. Đã kiểm thử 13.000 kịch bản ngẫu nhiên.

•      **Rate limiter**: tự giãn nhịp khi Steam trả lỗi 429 (quá nhiều request), tự thử lại khi lỗi mạng.

•      **Chia worker** theo appid % CRAWLER\_TOTAL\_WORKERS để nhiều tiến trình chạy song song trên cụm mà không trùng game.

•      State crawl lưu trong data/crawler\_state/reviews\_v2/. **Tại mỗi thời điểm chỉ một nơi được crawl**; chuyển máy hoặc lên cụm thì mang theo thư mục state.

## 4.2. Hàng đợi sự kiện (Kafka)

| Topic | Partition | Nội dung | Ghi chú |
| :---- | :---- | :---- | :---- |
| steam.reviews.raw | 6 | Review mới | Lượng lớn nhất nên nhiều partition nhất |
| steam.players.ccu | 3 | Snapshot CCU |   |
| steam.games.meta | 3 | Metadata, store, histogram | Compacted: chỉ giữ bản mới nhất theo key. Key tách theo loại (appid, appid:store, appid:histogram) để các loại không đè nhau |
| steam.alerts | 1 | Cảnh báo do S2/S3 sinh ra |   |

 

Trên cụm: 3 broker KRaft, mỗi message có 3 bản sao (RF=3), ghi thành công khi ít nhất 2 bản sao đã nhận (min.insync.replicas=2, producer acks=all). Nhờ đó mất một broker không mất dữ liệu đã xác nhận.

## 4.3. Batch layer (B1–B8)

Thứ tự chạy: **B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8 → merge\_views**. Thứ tự này không phải theo số thứ tự mà theo phụ thuộc dữ liệu:

| Job | Làm gì | Phụ thuộc / lý do vị trí |
| :---- | :---- | :---- |
| B1 Ingest | Đọc Kafka theo offset, ghi raw JSONL gzip theo ngày vào /steam/raw/.... Lưu offset ở /steam/checkpoints/b1\_offsets/ để lần sau đọc tiếp | Chạy đầu tiên. Khi dùng dữ liệu crawl local thì bỏ qua vì crawler đã ghi đúng layout raw |
| B2 Clean | Khử trùng review theo recommendationid, làm sạch CCU/metadata, tách tag và số review/ngày, gắn cờ review đáng ngờ, loại sự kiện demo | Mọi job sau đọc dữ liệu clean |
| B5 Cảm xúc | VADER cho điểm cảm xúc, từ điển khía cạnh, TF-IDF từ khoá (chỉ review tiếng Anh) | Chạy trước B3 vì B3 cần điểm cảm xúc trung bình |
| B3 Thống kê | Game theo ngày/tuần/tháng; theo thể loại, nhà phát triển, khoảng giá; theo ngôn ngữ | Tạo game\_daily\_stats cho B6 |
| B4 Giờ chơi | 5 nhóm giờ chơi, tỷ lệ chê sớm (\< 2 giờ), điểm đáng tiền |   |
| B6 Trending | So sánh 7 ngày gần nhất với 7 ngày trước (số review do Steam đếm), z-score, tác động giảm giá | Cần game\_daily\_stats của B3 |
| B7 Baseline | Tính mức "bình thường" của review mỗi giờ và CCU cùng khung giờ | S2, S3 đọc lại baseline này ở mỗi micro-batch |
| B8 Phân cụm | K-Means chia game thành 6 cụm | Chạy cuối |
| merge\_views | Xoá dữ liệu realtime đã được batch bao phủ, lưu serving\_state | Chỉ khi ghi Elasticsearch |

 

| Chạy lại an toàn (idempotent) B2–B8 luôn tính lại từ dữ liệu nguồn rồi ghi đè theo phân vùng ngày (dt). Chạy lại bao nhiêu lần cũng không sinh bản ghi trùng, không cần đổi thư mục đầu ra. B1 ghi raw trước rồi mới lưu offset. Nếu lỗi giữa hai bước, một phần dữ liệu có thể bị ghi lặp (at-least-once), nhưng B2 sẽ khử trùng. Hệ thống không tuyên bố exactly-once. |
| :---- |

 

## 4.4. Speed layer (S1–S3)

| Job | Cửa sổ thời gian | Làm gì | Ghi vào |
| :---- | :---- | :---- | :---- |
| S1 Metrics | 5 phút và 60 phút (trượt 5 phút) | Số review, % tích cực theo từng cửa sổ | ES realtime\_game\_metrics |
| S2 Review bombing | 60 phút, trượt 10 phút, watermark 15 phút | So số review tiêu cực với baseline 7 ngày | ES alerts \+ Kafka steam.alerts |
| S3 CCU | 10 phút | So CCU hiện tại với CCU trung bình cùng khung giờ | ES realtime\_game\_metrics, alerts |

 

**Watermark** là khoảng thời gian job chờ dữ liệu đến muộn. Với watermark 15 phút, review đến trễ hơn 15 phút so với cửa sổ của nó sẽ bị bỏ qua, để job không phải giữ trạng thái mãi.

Checkpoint của các job streaming lưu trên HDFS. Khi pod bị xoá, Kubernetes tạo lại pod và job đọc tiếp từ checkpoint. Kết quả ghi ES theo id cố định (upsert), nên xử lý lại một cửa sổ không sinh bản ghi trùng.

## 4.5. Luật phát hiện bất thường

### S2: Review bombing

Review bombing là khi nhiều người cùng lúc để lại review tiêu cực (thường vì một quyết định gây tranh cãi, không hẳn vì chất lượng game). Trong cửa sổ 60 phút, S2 cảnh báo khi **thoả cả ba** điều kiện:

1\.     Số review tiêu cực ≥ S2\_MIN\_NEGATIVE (mặc định 10).

2\.     Số review tiêu cực \> trung bình \+ 3 × độ lệch chuẩn của số review tiêu cực mỗi giờ trong 7 ngày (tính cả các giờ bằng 0).

3\.     % tích cực \< % tích cực baseline − 20 điểm.

Game chưa có baseline dùng ngưỡng dự phòng: ≥ 50 review và ≤ 35% tích cực. Độ lệch chuẩn theo giờ ước lượng từ dữ liệu theo ngày của histogram:

sigma\_gio \= sqrt( mean\_gio \+ Var(so\_review\_ngay) / 24^2 )

Nếu cửa sổ baseline có dưới 30 review, % tích cực baseline lấy theo tỷ lệ trọn đời Steam công bố, để tránh báo động giả ở game ít review.

### S3: CCU tăng/giảm đột ngột

Trong cửa sổ 10 phút, cảnh báo CCU\_SPIKE hoặc CCU\_DROP khi \|CCU − baseline\| / baseline \> 50%, với baseline là CCU trung bình cùng giờ (UTC) ở các ngày trước. Bỏ qua game có baseline \< 100 người. Khung giờ nào chưa từng thu CCU thì chưa có baseline, nên cần vòng thu chạy ít nhất 24 giờ (tốt nhất ≥ 14 ngày).

## 4.6. Serving layer và hợp nhất view

•      Kết quả batch ghi ra Parquet (/steam/curated/\*) và Elasticsearch (các index batch\_\*, reviews\_search, game\_keywords).

•      Kết quả streaming ghi vào Elasticsearch (realtime\_game\_metrics, alerts).

•      processing/serving/merge\_views.py xoá các cửa sổ realtime có window\_end \<= batch\_cutoff vì batch đã bao phủ, rồi lưu serving\_state.

•      **Cutoff là một thời điểm chính xác, tối đa 00:00 UTC hôm nay.** Nếu chỉ ghi ngày (lte: "YYYY-MM-DD"), Elasticsearch làm tròn lên cuối ngày và xoá nhầm cả realtime của hôm nay. Lỗi này đã gặp và đã sửa ngày 12/11.

## 4.7. Trực quan hoá

| Dashboard | Nội dung | Nguồn |
| :---- | :---- | :---- |
| D1 Tổng quan | KPI toàn hệ thống, xu hướng hài lòng, phân khúc | ES batch |
| D2 Chi tiết game | Lọc theo appid: review, cảm xúc, khía cạnh, giờ chơi | ES batch |
| D3 Realtime | Review và CCU trong 24 giờ gần nhất | ES realtime |
| D4 Cảnh báo & xu hướng | Cảnh báo S2/S3, trending, tác động giảm giá | ES alerts \+ batch |
| D5 Hệ thống (Grafana) | CPU, RAM, Kafka lag, trạng thái ES | Prometheus, chỉ có trên cụm |

 

Dashboard Kibana dùng Vega và được sinh từ code (dashboards/setup\_kibana.py), nên định nghĩa nằm trong Git và import lại được trên cụm mới.

# 5\. Tổ chức lưu trữ theo tầng

Dữ liệu đi qua 4 tầng, mỗi tầng sạch và tổng hợp hơn tầng trước:

| Tầng | Đường dẫn / index | Khoá | Ghi chú |
| :---- | :---- | :---- | :---- |
| Raw | /steam/raw/steam\_{reviews\_raw, players\_ccu, games\_meta}/dt= | — | JSON Lines gzip, không sửa (bất biến). Crawler local và B1 ghi cùng layout |
| Clean | clean/reviews/dt= | recommendationid | Giữ bản cập nhật mới nhất; có is\_english, is\_suspicious |
| Clean | clean/ccu/dt= | (appid, timestamp) | Thêm cột ts, hour |
| Clean | clean/games, clean/games\_history | appid / (appid, timestamp) | Gộp appdetails và store; lịch sử giá; price\_bucket |
| Clean | clean/review\_daily\_counts | (appid, dt) | Số review up/down theo ngày từ histogram |
| Curated | game\_daily\_stats, game\_period\_stats, segment\_stats, language\_stats | (appid, dt)... | B3 |
| Curated | review\_sentiment, aspect\_stats, keywords | — | B5 |
| Curated | playtime\_buckets, game\_retention | — | B4 |
| Curated | trending, trending\_top, event\_impact | (appid, dt) | B6 |
| Curated | review\_baseline, ccu\_baseline | appid / (appid, hour) | B7 |
| Curated | game\_clusters, cluster\_profiles | appid | B8 |
| Serving | ES batch\_\*, reviews\_search, game\_keywords | id ghép từ khoá | Upsert, chạy lại không trùng |
| Serving | ES realtime\_game\_metrics | metric\_type\_appid\_window\_size\_window\_start | Upsert theo cửa sổ |
| Serving | ES alerts \+ Kafka steam.alerts | alert\_type\_appid\_window\_start |   |

 

Mapping đầy đủ của Elasticsearch nằm ở storage/elasticsearch/indices\_mapping.json (15 index). Mọi mốc thời gian lưu trữ và ghi ES đều theo **UTC có múi giờ**.

# 6\. Môi trường triển khai

## 6.1. Hai môi trường

|   | Máy cá nhân (phát triển, demo) | Cụm cloud (nghiệm thu, kiểm thử) |
| :---- | :---- | :---- |
| Hạ tầng | WSL2 Ubuntu, khoảng 8 GB RAM | 4 VM Google Cloud e2-standard-2 (2 vCPU, 8 GB mỗi VM): 1 k3s server \+ 3 agent, nối bằng Tailscale |
| Spark | Local mode qua python pipeline.py ... | Client mode: driver trong pod CronJob/Deployment, executor là pod riêng |
| Kafka, ES, Kibana | Docker Compose (compose.demo.yaml), bật từng nhóm, mỗi container ≤ 1 GB | Strimzi (Kafka), ECK (ES, Kibana) |
| Lưu trữ | Thư mục local data/raw, data/lake | HDFS |
| Khi nào dùng | Viết/sửa job, làm dashboard, demo báo cáo tiến độ | Chạy trọn pipeline, đo SC1–SC4, FT1–FT5; chỉ bật trong buổi làm việc |

 

| Vì sao không dùng GKE? Manifest của nhóm dùng StorageClass local-path, nạp image trực tiếp vào node (imagePullPolicy: Never) và nhãn node riêng. Chuyển sang GKE phải dùng Artifact Registry, đổi StorageClass và nhãn, tức là làm lại phần hạ tầng. Script infra/k3s/install-node.sh cũng bắt buộc có interface tailscale0. |
| :---- |

 

## 6.2. Cấu hình thu gọn so với thiết kế gốc

| Thành phần | Thiết kế gốc | Đang dùng | Hệ quả |
| :---- | :---- | :---- | :---- |
| Kafka | 3 broker, RF=3, min ISR=2 | Giữ nguyên (KRaft) | Kiểm thử mất broker (FT1) có ý nghĩa |
| HDFS | 1 NameNode \+ 3 DataNode, replication 3 | 1 NameNode \+ 2 DataNode, replication 2 | Thử mất 1 DataNode được; NameNode là điểm lỗi đơn |
| Elasticsearch | 3 node, 1 replica | 1 node, 0 replica | Chưa làm được FT4/SC4; cần tạm nâng khi test |
| Hive Metastore | Có, kèm PostgreSQL | Chưa triển khai | Truy vấn ad-hoc bằng Spark SQL trên Parquet |
| Prometheus/Grafana | Có | Chưa triển khai | Tạm dùng kubectl top, Kafka consumer lag |
| Điều phối Spark | Spark Operator (SparkApplication) | CronJob/Deployment, client mode | Một image cho mọi job |

 

Tổng RAM ước tính của cấu hình thu gọn khoảng 15–16 GB trên 32 GB của cụm. Lưu ý: bộ triển khai đầy đủ (docs/deployment\_guide.md) cần 12–16 CPU, trong khi cụm chỉ có 8 vCPU, nên trên cụm chỉ chạy bộ thu gọn (k8s/fault-test).

## 6.3. Chịu lỗi theo thiết kế

| Thành phần | Cơ chế |
| :---- | :---- |
| Kafka | Nhân bản 3 bản sao, ghi cần 2 bản xác nhận, producer acks=all |
| HDFS | Mỗi block có nhiều bản sao trên các DataNode khác nhau (anti-affinity) |
| Elasticsearch | Replica shard trên node khác (cần ≥ 2 node) |
| Spark batch | Task tự chạy lại khi mất executor; job idempotent (ghi đè/upsert) |
| Spark streaming | Checkpoint trên HDFS; Deployment Recreate tự tạo lại driver; sink upsert |
| Crawler | Backoff khi gặp 429; thử lại khi lỗi mạng; state trên PVC nên bị ngắt không ghi trùng; CronJob chạy lại theo lịch |

 

# 7\. Các quyết định thiết kế và lý do

| Quyết định | Lý do |
| :---- | :---- |
| Ghi ES bằng Python client (processing/common/es\_writer.py) thay cho connector JVM elasticsearch-spark | Ít phụ thuộc phiên bản Scala/Spark; lỗi ghi được báo ra thay vì bị bỏ qua |
| Hive Metastore 3.1.3 (nếu triển khai) thay vì 4.x | Spark 3.5 dùng client Hive 2.3, không tương thích HMS 4 |
| Spark client mode thay cho SparkApplication | Một image cho mọi job; dùng chung pipeline.py ở local và trên cụm |
| Tên thư mục raw theo topic (steam\_reviews\_raw...) | Dữ liệu crawl local và đầu ra B1 có cùng layout, dùng thẳng được |
| Đếm review theo ngày bằng histogram của Steam | Review đã crawl ở game lớn chỉ là mẫu gần nhất (10 trang phủ vài giờ đến vài ngày), không dùng để đếm được |
| Mọi thời gian ghi ES là UTC có múi giờ | PySpark trả thời gian theo giờ máy; ES hiểu chuỗi không múi giờ là UTC, dẫn tới lệch 7 giờ |
| Chỉ phân tích nội dung review tiếng Anh | VADER và từ điển khía cạnh chỉ hỗ trợ tiếng Anh; ngôn ngữ khác chỉ dùng nhãn thích/không thích |
| Sự kiện demo bơm qua pipeline.py inject với source=demo\_injection | S2/S3 xử lý như thường để demo cảnh báo; B2 loại các bản ghi này nên không lẫn vào phân tích |
| Không dùng replay \--retime để demo realtime | Tuỳ chọn này dồn review cũ về hiện tại, làm game nhỏ trông như bị bombing; chỉ dùng cho thử tải SC3 |

 

# 8\. Thuật ngữ

| Thuật ngữ | Nghĩa |
| :---- | :---- |
| appid | Mã số duy nhất của một game trên Steam (ví dụ 730 là Counter-Strike 2\) |
| CCU | Concurrent users: số người đang chơi tại một thời điểm |
| Review bombing | Nhiều review tiêu cực dồn dập trong thời gian ngắn |
| Baseline | Mức "bình thường" của một chỉ số, dùng để so khi phát hiện bất thường |
| Partition (Kafka) | Một phần của topic; nhiều partition cho phép nhiều consumer đọc song song |
| Offset | Vị trí của message trong partition; lưu offset để lần sau đọc tiếp |
| Micro-batch | Cách Spark Streaming xử lý: gom sự kiện vài giây/chục giây thành một lô nhỏ |
| Idempotent | Chạy lại nhiều lần cho cùng kết quả, không sinh dữ liệu trùng |
| Upsert | Ghi đè nếu id đã tồn tại, thêm mới nếu chưa |
| dt | Cột/thư mục phân vùng theo ngày (YYYY-MM-DD, UTC) |
| Cutoff | Mốc thời gian mà batch đã tính xong; realtime trước mốc này bị xoá khi hợp nhất |
| Pod, Deployment, CronJob, PVC | Khái niệm Kubernetes, xem tài liệu "Chức năng các công cụ" |

 

**STEAM PLAYER INSIGHTS**

**Các công cụ sử dụng**

Mỗi công cụ là gì, dùng vào việc gì trong dự án và cần lưu ý gì 

# **1\. Bản đồ công cụ theo tầng**

| Tầng | Công cụ | Vai trò một câu |
| :---- | :---- | :---- |
| Nguồn dữ liệu | Steam Web API, Steam Store API | Nơi lấy review, metadata, CCU |
| Thu thập | Python crawler (requests, kafka-python) | Gọi API theo lịch, ghi ra file hoặc Kafka |
| Hàng đợi sự kiện | Apache Kafka (KRaft) | Bộ đệm sự kiện bền vững giữa crawler và Spark |
| Lưu trữ thô/sạch | HDFS, định dạng JSONL gzip và Parquet | Kho dữ liệu phân tán, giữ dữ liệu gốc và dữ liệu đã làm sạch |
| Xử lý | Apache Spark 3.5 (PySpark): Spark SQL, Structured Streaming, MLlib | Tính toán batch và streaming trên nhiều máy |
| Phân tích văn bản | VADER, từ điển khía cạnh, TF-IDF, K-Means | Cảm xúc, khía cạnh bị phàn nàn, từ khoá, phân cụm game |
| Truy vấn | Spark SQL, Hive Metastore (chưa triển khai) | Truy vấn ad-hoc trên Parquet |
| Phục vụ | Elasticsearch | Lưu kết quả để dashboard truy vấn nhanh, tìm kiếm review |
| Hiển thị | Kibana (Vega), Grafana, Jupyter | Dashboard nghiệp vụ, dashboard hệ thống, notebook phân tích |
| Giám sát | Prometheus, kube-prometheus-stack | Thu số liệu tài nguyên cho Grafana D5 |
| Hạ tầng | Kubernetes (k3s), Strimzi, ECK, Spark Operator, Helm | Chạy và quản lý mọi thành phần trên cụm |
| Mạng, máy | Google Cloud Compute Engine, Tailscale | 4 máy ảo và mạng riêng nối chúng |
| Phát triển | Conda/venv, Java 17, Docker Compose, Make, pipeline.py, pytest, kubeconform | Chạy thử trên máy cá nhân, kiểm thử |

 

# **2\. Nguồn dữ liệu: Steam API**

Steam cung cấp hai nhóm API qua HTTP, trả về JSON:

•      **Steam Web API** (api.steampowered.com): các dịch vụ như IStoreService, ISteamChartsService, IStoreBrowseService, IUserReviewsService, ISteamUserStats. Một số endpoint cần **API key** (STEAM\_API\_KEY).

•      **Store API** (store.steampowered.com): appdetails (giá, thể loại...) và appreviewhistogram (số review theo ngày).

Danh sách đầy đủ 8 endpoint và dữ liệu lấy được nằm ở tài liệu "Mô tả dự án và kiến trúc", mục 2.2.

| Lưu ý khi dùng •      Giữ STEAM\_API\_KEY thật trong file .env ở gốc repo. Không commit, không in ra log. •      Tốc độ an toàn khoảng 1 request / 1,5 giây (CRAWLER\_RATE\_LIMIT\_DELAY=1.5). Nhiều tiến trình chạy cùng lúc sẽ cộng dồn tốc độ và dễ bị chặn (lỗi 429). •      STEAM\_WEB\_API\_BASE, STEAM\_STORE\_API\_BASE chỉ đổi khi kiểm thử với server giả (mock). |
| :---- |

 

# **3\. Thu thập: Python crawler**

Các script trong crawlers/ gọi Steam API, chuẩn hoá bản ghi và ghi ra một trong hai đích:

•      **File JSONL gzip** (CRAWLER\_OUTPUT=jsonl): mỗi dòng một bản ghi JSON, nén gzip, chia theo thư mục ngày. Dùng khi phát triển và để chia sẻ dữ liệu cho nhóm.

•      **Kafka** (CRAWLER\_OUTPUT=kafka): đẩy thẳng vào topic, dùng khi demo streaming.

| Script | Chức năng |
| :---- | :---- |
| build\_targets.py | Tạo hoặc mở rộng danh sách game theo dõi (data/target\_apps.json \+ manifest) |
| collect\_local.py | Chạy tuần tự các nguồn store → metadata → histogram → ccu → reviews, luôn ghi JSONL local, ghi báo cáo tiến độ collection-worker-0.json |
| steam\_\*\_crawler.py | Crawler riêng từng nguồn, chạy được với Kafka; dùng cho CronJob trên cụm |
| update\_targets\_from\_trending.py | Vòng phản hồi: thêm game đang lên trên bảng most-played |
| demo\_inject.py | Bơm sự kiện có nhãn source=demo\_injection vào Kafka để demo cảnh báo |
| replay\_jsonl\_to\_kafka.py | Phát lại file raw vào Kafka (nạp dữ liệu, thử tải SC3) |
| common/review\_state.py | Lưu state crawl review tăng dần, không trùng |

 

Thư viện chính: requests (gọi HTTP) và kafka-python (producer gửi message vào Kafka). Lưu ý kafka-python chỉ dùng cho crawler; Spark đọc Kafka bằng connector riêng spark-sql-kafka.

# **4\. Hàng đợi sự kiện: Apache Kafka**

## **4.1. Kafka là gì?**

Kafka là một **nhật ký sự kiện phân tán**: bên ghi (producer) thêm message vào cuối, bên đọc (consumer) đọc theo vị trí riêng của mình. Message không bị xoá ngay khi đọc, nên nhiều bên có thể đọc cùng một dữ liệu, và một bên bị lỗi có thể đọc lại từ vị trí cũ.

Trong dự án, Kafka **tách crawler khỏi Spark**: crawler cứ gửi dữ liệu dù Spark đang bận hay đang khởi động lại; cùng một luồng review được cả B1 (batch) và S1/S2 (streaming) đọc độc lập.

## **4.2. Khái niệm cần biết**

| Khái niệm | Giải thích | Trong dự án |
| :---- | :---- | :---- |
| Topic | Một luồng message cùng loại | 4 topic: steam.reviews.raw, steam.players.ccu, steam.games.meta, steam.alerts |
| Partition | Topic được chia thành nhiều phần để đọc/ghi song song | 6 / 3 / 3 / 1 partition trên cụm; 1 partition khi chạy local |
| Broker | Một máy chủ Kafka | 3 broker trên cụm, 1 broker local |
| Replication factor (RF) | Số bản sao của mỗi partition trên các broker khác nhau | RF=3 |
| min.insync.replicas | Số bản sao tối thiểu phải nhận được thì mới coi là ghi thành công | 2: mất 1 broker vẫn ghi được, không mất dữ liệu |
| acks=all | Producer chờ mọi bản sao đồng bộ xác nhận | Bật ở crawler |
| Offset | Số thứ tự message trong partition | B1 tự lưu offset; S1–S3 lưu trong checkpoint |
| Compacted topic | Chỉ giữ message mới nhất cho mỗi key | steam.games.meta |
| KRaft | Chế độ Kafka tự quản lý metadata, không cần ZooKeeper | Dùng cả local và cụm |
| Consumer lag | Số message chưa được đọc | Dùng làm số đo khi kiểm thử tải SC3 |

 

Trên cụm, Kafka được cài bằng **Strimzi** (operator, xem mục 11). Trên máy cá nhân dùng image apache/kafka:3.9.1 qua Docker Compose: bash scripts/demo.sh up streaming.

# **5\. Lưu trữ phân tán: HDFS và định dạng file**

## **5.1. HDFS**

HDFS (Hadoop Distributed File System) là hệ thống file trải trên nhiều máy. File lớn được cắt thành các **block**, mỗi block được nhân bản lên nhiều máy, nên mất một máy không mất dữ liệu.

| Thành phần | Vai trò | Trong dự án |
| :---- | :---- | :---- |
| NameNode | Giữ "mục lục": file nào gồm block nào, block nằm ở đâu. Không giữ dữ liệu thật | 1 NameNode (là điểm lỗi đơn của cấu hình thu gọn) |
| DataNode | Lưu block dữ liệu thật | 2 DataNode trên cụm, 1 khi chạy local |
| Replication | Số bản sao mỗi block | 2 trên cụm (thiết kế gốc 3), 1 khi chạy local |

 

Các thư mục chính: /steam/raw, /steam/clean, /steam/curated, /steam/checkpoints. Trên máy cá nhân **không bắt buộc HDFS**: profile local của pipeline.py đọc data/raw và ghi data/lake.

| Lưu ý •      Chỉ chạy hdfs namenode \-format một lần với cụm mới. Format lại sẽ mất toàn bộ dữ liệu. •      Script storage/hdfs/setup\_hdfs\_dirs.sh đặt replication 3, không dùng nguyên script này cho HDFS local một DataNode. •      HDFS kém hiệu quả với nhiều file nhỏ (NameNode phải nhớ từng file). Hiện có 1.020 file raw review và 10.737 file Parquet; cần gộp trước khi mở rộng. |
| :---- |

 

## **5.2. Định dạng file**

| Định dạng | Đặc điểm | Dùng ở |
| :---- | :---- | :---- |
| JSON Lines gzip (.jsonl.gz) | Mỗi dòng một JSON, nén gzip. Dễ ghi nối tiếp, giữ nguyên cấu trúc gốc của API | Tầng raw |
| Parquet | Lưu theo cột, có nén và schema. Đọc vài cột rất nhanh, nhỏ hơn JSON nhiều lần | Tầng clean và curated |

 

Dữ liệu được **phân vùng theo ngày** (dt=YYYY-MM-DD): mỗi ngày một thư mục, nên job chỉ cần đọc những ngày liên quan và ghi đè đúng ngày cần tính lại.

# **6\. Xử lý: Apache Spark (PySpark 3.5.7)**

## **6.1. Spark là gì?**

Spark là bộ máy tính toán phân tán: một chương trình được chia thành nhiều task chạy song song trên nhiều máy. Nhóm viết job bằng Python (PySpark), chủ yếu dùng DataFrame và Spark SQL.

| Khái niệm | Giải thích |
| :---- | :---- |
| Driver | Tiến trình điều phối: đọc code, chia việc, tổng hợp kết quả |
| Executor | Tiến trình làm việc thật, chạy các task. Trên cụm mỗi executor là một pod (1 CPU, 2 GB) |
| Partition (Spark) | Một phần dữ liệu được xử lý bởi một task. Quy tắc: số shuffle partition khoảng 2–4 lần tổng số core |
| Shuffle | Trao đổi dữ liệu giữa các executor (khi groupBy, join). Tốn nhiều tài nguyên nhất |
| Local mode | Driver và executor cùng một tiến trình trên một máy (local\[4\] \= 4 luồng). Dùng khi phát triển |
| Client mode | Driver chạy trong pod CronJob/Deployment, executor là pod do Spark tạo trên Kubernetes |
| Dynamic allocation | Tự tăng/giảm số executor theo tải; trên cụm từ 2 đến 6 pod |

 

## **6.2. Spark trong batch layer**

Job B1–B8 nằm ở processing/batch/. Spark SQL/DataFrame dùng cho làm sạch, khử trùng, tổng hợp theo ngày/tuần/tháng, cửa sổ thời gian theo lịch (rangeBetween) để tính 7 ngày gần nhất, và z-score. Chạy toàn bộ:

python pipeline.py \--profile local batch \--raw data/raw \--lake data/lake \--es

## **6.3. Spark Structured Streaming**

Job S1–S3 nằm ở processing/streaming/. Structured Streaming coi luồng Kafka như một bảng không ngừng thêm dòng, và xử lý theo **micro-batch** (mỗi trigger, ví dụ 20 giây, gom các message mới thành một lô nhỏ).

•      **Window**: gom sự kiện theo khung thời gian (5 phút, 60 phút trượt 10 phút...).

•      **Watermark**: thời gian chờ dữ liệu đến muộn (S2 dùng 15 phút); quá hạn thì bỏ qua.

•      **Checkpoint**: lưu offset và trạng thái trên HDFS, để job khởi động lại đọc tiếp đúng chỗ.

•      Cần connector org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.7; lần đầu cần mạng để tải JAR.

## **6.4. Spark MLlib**

Dùng cho TF-IDF (B5), K-Means (B8, 6 cụm) và Logistic Regression phân loại cảm xúc review (B9, xem mục 6 phần phân công). Đây là phần học máy ở mức cơ bản, phù hợp với yêu cầu môn học không tập trung vào mô hình.

# **7\. Phân tích văn bản và thống kê**

| Kỹ thuật | Làm gì | Ở đâu | Hạn chế |
| :---- | :---- | :---- | :---- |
| VADER | Bộ từ điển cảm xúc tiếng Anh: cho mỗi review điểm từ −1 (rất tiêu cực) tới \+1 (rất tích cực), không cần huấn luyện | B5 | Chỉ tiếng Anh; chưa đánh giá độ chính xác; khó với mỉa mai |
| Từ điển khía cạnh | Danh sách từ khoá cho từng khía cạnh (đồ hoạ, hiệu năng, giá, lỗi, server...). Review chứa từ khoá được gắn khía cạnh đó | B5 | Phụ thuộc chất lượng từ điển |
| TF-IDF | Đánh trọng số từ: từ xuất hiện nhiều trong review của một game nhưng hiếm ở game khác thì quan trọng | B5 | Không hiểu ngữ cảnh |
| K-Means | Chia game thành k cụm có đặc trưng gần nhau (k \= 6\) | B8 | Cần chuẩn hoá đặc trưng; cụm 1 game cần xem lại |
| z-score | (giá trị − trung bình) / độ lệch chuẩn: đo một số liệu khác thường bao nhiêu | B6 |   |
| Luật 3σ | Cảnh báo khi giá trị vượt trung bình \+ 3 độ lệch chuẩn | S2 | Với số đếm nhỏ thỉnh thoảng báo ở game nhỏ; chỉnh bằng S2\_SIGMA, S2\_MIN\_NEGATIVE |

 

# **8\. Truy vấn: Spark SQL và Hive Metastore**

•      **Spark SQL** cho phép viết SQL trên file Parquet. Hiện nhóm đăng ký Parquet thành bảng tạm bằng createOrReplaceTempView rồi truy vấn. Câu mẫu ở storage/hive/adhoc\_queries.sql.

•      **Hive Metastore** là "danh bạ bảng": lưu tên bảng, schema, vị trí file trên HDFS (trong PostgreSQL), để mọi phiên Spark cùng thấy một bộ bảng steam\_analytics mà không phải đăng ký lại.

•      Phiên bản: Hive Metastore **3.1.3**, vì Spark 3.5 dùng client Hive 2.3, không tương thích HMS 4.x (lỗi Invalid method name: 'get\_table').

•      Trạng thái: **chưa triển khai** trong cấu hình thu gọn. Job tự đăng ký bảng nếu sau này có Metastore. Cần hỏi giảng viên phương án này có đáp ứng yêu cầu lớp truy vấn không.

# **9\. Phục vụ: Elasticsearch**

Elasticsearch (ES) là cơ sở dữ liệu tìm kiếm và phân tích: truy vấn lọc, tổng hợp theo thời gian và tìm kiếm toàn văn rất nhanh. Dashboard đọc từ ES thay vì đọc Parquet trực tiếp vì Parquet không phù hợp cho truy vấn tương tác liên tục.

| Khái niệm | Giải thích | Trong dự án |
| :---- | :---- | :---- |
| Index | Tương đương một bảng | 15 index: batch\_\*, reviews\_search, game\_keywords, realtime\_game\_metrics, alerts, serving\_state... |
| Document | Một bản ghi JSON trong index | Ví dụ 19.861 document trong batch\_game\_daily |
| Mapping | Schema của index (kiểu từng trường) | storage/elasticsearch/indices\_mapping.json |
| Shard / replica | Index chia thành shard; replica là bản sao shard trên node khác | Thiết kế 3 node, 1 replica; đang dùng 1 node, 0 replica |
| Upsert theo id | Id ghép từ khoá; ghi lại cùng id thì ghi đè | Giúp chạy lại không trùng |

 

•      Ghi bằng Python client (processing/common/es\_writer.py), bulk theo partition. Bật bằng WRITE\_ES=1 hoặc \--es-nodes localhost:9200.

•      Mọi trường thời gian ghi dạng UTC có múi giờ.

•      Trên cụm cài bằng **ECK** (Elastic Cloud on Kubernetes). Mật khẩu user elastic nằm trong secret steam-elasticsearch-es-elastic-user.

•      Kiểm thử SC4 (tải truy vấn) và FT4 (mất node) cần ít nhất 2–3 node ES.

# **10\. Hiển thị và giám sát**

## **10.1. Kibana**

Giao diện web của Elasticsearch. Nhóm dựng 4 dashboard D1–D4 bằng **Vega** (ngôn ngữ mô tả biểu đồ dạng JSON), sinh từ code dashboards/setup\_kibana.py. Lợi ích: dashboard nằm trong Git, dựng lại trên cụm mới chỉ cần chạy lại script. Truy cập local: http\://localhost:5601.

## **10.2. Prometheus và Grafana**

•      **Prometheus** định kỳ thu số liệu (CPU, RAM, Kafka lag, trạng thái ES) từ các thành phần. Cài bằng Helm chart kube-prometheus-stack, kèm Elasticsearch exporter và PodMonitor cho Kafka.

•      **Grafana** vẽ dashboard từ Prometheus. Dashboard D5 định nghĩa ở dashboards/grafana/d5\_system.json.

•      Trạng thái: **chưa triển khai** trong cấu hình thu gọn; tạm dùng kubectl top và Kafka consumer lag.

## **10.3. Jupyter Notebook**

Dùng cho EDA, phân tích cảm xúc và vẽ biểu đồ kết quả SC/FT. Có 3 notebook, chưa chạy lại trên bộ dữ liệu mới.

# **11\. Hạ tầng: Kubernetes (k3s) và các operator**

## **11.1. Kubernetes và k3s**

Kubernetes (K8s) điều phối container trên nhiều máy: tự đặt chương trình lên máy còn tài nguyên, tự khởi động lại khi chết, tự chuyển sang máy khác khi một máy hỏng. **k3s** là bản Kubernetes gọn nhẹ (một file cài, ít RAM), phù hợp cụm nhỏ. k3s dùng **containerd** làm container runtime, không cần Docker.

| Đối tượng K8s | Giải thích | Dùng cho |
| :---- | :---- | :---- |
| Node | Một máy trong cụm (server điều khiển hoặc agent chạy việc) | 4 VM: 1 server \+ 3 agent |
| Pod | Đơn vị chạy nhỏ nhất, gồm 1 hoặc vài container | Mỗi broker, DataNode, executor... |
| Deployment | Giữ một số pod luôn chạy; pod chết thì tạo lại | steam-s1, steam-s2, steam-s3 (chiến lược Recreate) |
| StatefulSet | Như Deployment nhưng pod có tên và ổ đĩa cố định | HDFS (ví dụ pod hdfs-namenode-0), Elasticsearch; Kafka do Strimzi quản lý theo cách tương tự |
| CronJob / Job | Chạy một việc theo lịch / một lần | steam-batch-daily (01:30 UTC), crawler định kỳ |
| PVC / StorageClass | Yêu cầu ổ đĩa bền vững / loại ổ đĩa | crawler-state-pvc; StorageClass local-path của k3s |
| ConfigMap / Secret | Cấu hình / thông tin bí mật đưa vào pod | steam-pipeline-config (ngưỡng cảnh báo), spark-defaults; steam-api, steam-es-credentials |
| Namespace | Nhóm tài nguyên riêng | kafka, hdfs, elastic-system, spark-jobs, monitoring |
| requests / limits | Tài nguyên tối thiểu được giữ / tối đa được dùng | Đặt cho mọi pod; JVM: limits gấp 1,5–2 lần requests |
| ResourceQuota | Giới hạn tổng tài nguyên một namespace | Chống một nhóm pod chiếm hết cụm |
| Readiness probe | Kiểm tra pod đã sẵn sàng nhận traffic chưa | Mọi container |
| Anti-affinity | Quy tắc không đặt các pod cùng loại lên cùng node | Rải broker, DataNode, ES lên các node khác nhau để mất 1 node không mất hết |

 

## **11.2. Operator và Helm**

| Công cụ | Vai trò |
| :---- | :---- |
| Strimzi 0.45 | Operator cài và vận hành Kafka trên K8s: chỉ cần khai báo resource Kafka, KafkaTopic, Strimzi tự dựng broker, topic |
| ECK 2.11.1 | Operator của Elastic, dựng Elasticsearch và Kibana từ resource khai báo, tự tạo chứng chỉ và mật khẩu |
| Spark Operator | Cho phép chạy Spark bằng resource SparkApplication. Đã cài nhưng nhóm chọn client mode nên chưa dùng |
| Helm v3 | Trình quản lý gói cho K8s; dùng để cài các operator và kube-prometheus-stack |
| kubectl | Công cụ dòng lệnh điều khiển cụm (get pods, logs, scale, rollout restart...) |

 

## **11.3. Script triển khai trong repo**

| Script / thư mục | Chức năng |
| :---- | :---- |
| infra/k3s/README.md, install-node.sh | Dựng cụm k3s qua Tailscale (bộ thu gọn) |
| infra/k3s/FAULT\_TEST.md | Kịch bản kiểm thử chịu lỗi FT1–FT6 |
| k8s/demo, k8s/fault-test | Manifest bộ thu gọn |
| scripts/setup\_cluster.sh | Cài operator và monitoring (bộ đầy đủ) |
| scripts/deploy\_all.sh | Dựng Kafka, HDFS, ES, Hive, build image, tạo CronJob/Deployment, index, dashboard (bộ đầy đủ) |
| scripts/teardown.sh | Gỡ hệ thống; DELETE\_DATA=1 xoá cả PVC (không khôi phục được) |

 

| Image Spark trên k3s chứa bản sao code lúc build Mỗi khi processing/ hoặc crawlers/ thay đổi phải build lại image, tăng tag (dạng k3s-YYYYMMDD) và sửa tag ở mọi manifest. Tag hiện tại localhost/steam-spark-demo:k3s-20261026 đã cũ hơn code ngày 12/11. |
| :---- |

 

# **12\. Máy và mạng: Google Cloud và Tailscale**

•      **Google Cloud Compute Engine**: 4 VM e2-standard-2 (2 vCPU, 8 GB), Ubuntu 24.04 LTS, tổng 8 vCPU / 32 GB. Dùng GCP Free Trial (300 USD / 90 ngày, giới hạn khoảng 8 vCPU chạy cùng lúc).

•      Chi phí khoảng 0,25–0,3 USD/giờ cho cả cụm cộng tiền ổ đĩa; bật 24/7 tốn khoảng 200 USD/tháng. **Chỉ bật trong buổi làm việc, stop ngay khi xong**, và đặt Budget alert.

•      **Tailscale**: VPN dạng mesh, cho các máy ở mạng khác nhau thấy nhau như cùng mạng LAN. Node IP và Flannel (mạng pod của k3s) đi qua interface tailscale0. Giữ Tailscale giúp ghép thêm máy cá nhân vào cụm khi cần.

•      Phương án dự phòng: Azure for Students (100 USD) → ghép máy cá nhân qua Tailscale → Oracle Cloud Always Free (ARM, phải build image ARM64).

# **13\. Công cụ phát triển trên máy cá nhân**

| Công cụ | Chức năng | Lưu ý |
| :---- | :---- | :---- |
| WSL2 Ubuntu | Môi trường Linux trên Windows | Chạy mọi lệnh trong WSL; đặt repo trong filesystem Linux |
| Conda hoặc venv, Python 3.11 | Môi trường Python riêng cho dự án | Chọn một trong hai; pip install \-r requirements.txt (đã khoá phiên bản) |
| JDK 17 | Spark và Hadoop chạy trên JVM | sudo apt install openjdk-17-jdk |
| Docker Compose (compose.demo.yaml) | Bật Kafka, HDFS, ES/Kibana trên một máy theo nhóm (batch, streaming, dashboard) | Chỉ dùng để phát triển và demo; nghiệm thu chạy trên k3s |
| scripts/demo.sh | Bật/tắt dịch vụ và streaming: up, stream-start, stream-stop, stream-status, reset-kafka | Dừng streaming khi chạy batch để đủ RAM |
| pipeline.py | Điểm chạy chung: batch, b1…b8, s1…s3, merge, inject, replay, feedback | Dùng chung cho local và cụm (\--profile) |
| Make (Makefile) | Lối tắt cho các lệnh pipeline (make batch, make s2...) | Cùng thứ tự job với pipeline.py |
| tests/make\_sample\_data.py | Sinh dữ liệu giả 30 game × 21 ngày, có sẵn bombing, CCU spike, đợt giảm giá | Thử logic không cần Steam API |
| pytest | Chạy unit test (tests/unit, 36 test) | python \-m pytest \-q tests/unit |
| kubeconform | Kiểm tra schema manifest K8s mà không cần cụm | Đã đạt 32/32 ngày 26/10 |

 

## **13.1. RAM đo được trên một máy (12/11/2026)**

| Thành phần | Cấu hình | RAM thực tế |
| :---- | :---- | :---- |
| Kafka 1 broker | heap 512 MB | khoảng 280 MB |
| Elasticsearch 1 node | heap 512 MB | khoảng 815 MB |
| Kibana | giới hạn 1 GB | khoảng 490 MB |
| Spark batch B2–B8 | local\[4\], driver 2–3 GB | 2–3 GB, khoảng 12 phút |
| Mỗi job streaming | local\[2\], driver 1 GB | khoảng 1 GB |
| Crawler | Python | \< 0,5 GB |

 

**STEAM PLAYER INSIGHTS**

**Báo cáo tiến độ**

Trạng thái dự án tại ngày đối chiếu 12/11/2026

**Môi trường chạy:** Một máy WSL2, 8 GB RAM, dữ liệu thật từ API Steam

**Nguồn số liệu:** Đếm trực tiếp từ data/raw, data/lake, Elasticsearch và tests/results/ ngày 12/11/2026 

# **1\. Tóm tắt**

**Toàn bộ luồng Lambda đã chạy thông trên dữ liệu thật**: Steam API → Kafka → Spark (8 job batch, 3 job streaming) → Elasticsearch → Kibana D1–D4. Tuy nhiên mới chạy **trên một máy với 100 game**. Phần chứng minh hệ thống phân tán (chạy trên cụm, đo khả năng mở rộng và chịu lỗi) chưa làm, và lịch sử CCU mới bắt đầu được thu.

| Cách đọc màu trong bảng •      Xanh \= Xong: có code, đã chạy và có kết quả trong ngày đối chiếu. •      Vàng \= Một phần: có code/script, mới chạy trên một máy hoặc chưa đủ bằng chứng. •      Đỏ \= Chưa: chưa chạy ở môi trường yêu cầu hoặc chưa bắt đầu. |
| :---- |

 

## **1.1. Đã làm được**

•      **Thay toàn bộ dữ liệu**, chỉ lấy từ API chính thức của Steam: 100 game chọn có phương pháp, 92.019 review không trùng, 100/100 game có review.

•      **Thu thập tăng dần, không trùng**: lần sau chỉ lấy review mới rồi lấy tiếp review cũ; danh sách game chỉ mở rộng. Kiểm thử 13.000 kịch bản ngẫu nhiên.

•      **Batch B2→B8** chạy thành công trên dữ liệu thật, ghi Elasticsearch (khoảng 12 phút).

•      **Streaming S1–S3** chạy với Kafka: review và CCU thật đi vào ES; tình huống bơm có nhãn đều bị phát hiện; không có báo động giả trên dữ liệu thường.

•      **Dashboard Kibana D1–D4**: 18/19 panel có dữ liệu (panel còn lại trống đúng).

•      **Kiểm thử**: 36/36 unit test đạt; FT6 đạt; SC1, SC3, SC4 chạy thử trên một máy.

•      Sửa 7 lỗi lớn phát hiện khi chạy thật (mục 8).

## **1.2. Còn thiếu quan trọng nhất**

•      Chưa chạy trên cụm Kubernetes nhiều node; image Spark trên cụm phải build lại.

•      Chưa có số đo SC1–SC4 và FT1–FT5 trên cụm; chưa đo độ trễ streaming end-to-end (yêu cầu ≤ 2 phút).

•      CCU mới có 2 snapshot mỗi game, chưa có xu hướng CCU; cần thu liên tục ≥ 14 ngày.

•      Quy mô demo 100 game nhỏ hơn thiết kế 1.000–2.000 game.

•      Notebook chưa chạy trên dữ liệu mới; chưa có báo cáo cuối kỳ, slide, video.

## **1.3. Tổng hợp theo giai đoạn**

| Giai đoạn | Trạng thái | Đã kiểm chứng | Còn lại |
| :---- | :---- | :---- | :---- |
| 0\. Thu thập dữ liệu | Xong | Crawl thật 100 game, 92.019 review, 0 trùng; FT6 đạt | Vòng thu liên tục; mở rộng danh sách; CronJob store/histogram |
| 1\. Batch layer B1–B8 | Xong | B2–B8 trên dữ liệu thật, ghi ES; B1 Kafka → raw local | Chạy trên cụm, lịch hằng ngày |
| 2\. Speed layer S1–S3 \+ hợp nhất | Xong (local) | S1–S3 với Kafka, dữ liệu thật và tình huống bơm; merge\_views giữ đúng realtime hôm nay | Đo độ trễ; chạy trên cụm |
| 3\. Truy vấn & phân tích bổ sung | Một phần | K-Means, review đáng ngờ, vòng phản hồi mới | Hive Metastore chưa triển khai |
| 4\. Dashboard | Một phần | D1–D4: 18/19 panel có dữ liệu thật | Ảnh chụp; Grafana D5 chỉ có trên cụm |
| 5\. Kubernetes | Có code | kubeconform 32/32 (26/10) | Build image mới, thêm CronJob crawler, dựng cụm 4 VM |
| 6\. Kiểm thử SC/FT | Có công cụ | Unit 36/36; FT6 đạt; SC1/SC3/SC4 chạy thử một máy | SC1–SC4, FT1–FT5 trên cụm |
| 7\. Bàn giao | Chưa | Tài liệu đã cập nhật | Notebook, báo cáo cuối, slide, video |

 

# **2\. Đối chiếu với yêu cầu môn học**

| Yêu cầu trong slide | Repo đang có | Mức đáp ứng |
| :---- | :---- | :---- |
| Kiến trúc đủ thành phần: thu thập, lưu trữ, phân tích, trực quan hoá | Crawler → Kafka/file → raw/clean/curated → Spark batch \+ streaming → ES → Kibana; chạy toàn trình 12/11 | Xong (một máy) |
| Dashboard giúp hiểu tổng quan dữ liệu | Kibana D1 Tổng quan, D2 Chi tiết game, D3 Realtime, D4 Cảnh báo & xu hướng | Xong (D1–D4) |
| Phân tích: tìm thông tin ẩn, giải quyết vấn đề thực tế | Cảm xúc, khía cạnh, TF-IDF, giờ chơi, trending, bất thường, phân cụm | Một phần: chưa viết kết luận 5 câu hỏi |
| Môi trường K8s hoặc cloud, không Docker | k3s trên 4 VM Google Cloud (đã chốt); Docker Compose chỉ để phát triển | Có code, chưa dựng |
| Mô phỏng môi trường phân tán | Kafka 3 broker, HDFS 1 NN \+ 2 DN, executor theo pod, pod rải trên các node | Có code, chưa dựng |
| Kiểm thử scalability | SC1 executor, SC2 dữ liệu, SC3 tải streaming, SC4 tải truy vấn ES | Chạy thử một máy |
| Kiểm thử fault-tolerance | FT1 broker, FT2 DataNode, FT3 executor/driver, FT4 node ES, FT5 worker, FT6 lỗi 429 | FT6 đạt; FT1–FT5 chưa |
| Không tập trung vào mô hình ML | VADER, từ điển, K-Means đơn giản | Xong |

 

# **3\. Chức năng F1–F6**

| Mã | Yêu cầu thiết kế | Kết quả 12/11/2026 | Còn lại |
| :---- | :---- | :---- | :---- |
| F1 | Crawl định kỳ review, metadata, CCU; Kafka; backfill | Đã chạy. 8 endpoint Steam; crawl tăng dần; crawler đẩy thẳng Kafka khi demo | Vòng thu liên tục ≥ 14 ngày; CronJob store/histogram |
| F2 | Raw bất biến trên HDFS; clean Parquet | Đã chạy local. Raw 18,3 MiB; 5 bảng clean | Chưa chạy HDFS trên cụm |
| F3 | Batch tổng hợp, cảm xúc, khía cạnh, xu hướng | Đã chạy. B2–B8 thành công; trending dùng số review/ngày của Steam | Lịch hằng ngày trên cụm |
| F4 | Streaming cửa sổ, bất thường, cảnh báo trong vài phút | Đã chạy local với Kafka; cảnh báo xuất hiện khoảng 1 phút sau khi bơm | Đo độ trễ chuẩn; chạy trên cụm |
| F5 | SQL ad-hoc; tìm kiếm review | reviews\_search 34.725 review; Spark SQL đọc Parquet | Hive chưa có; lưu ví dụ truy vấn thật |
| F6 | Dashboard tổng quan, chi tiết, realtime, cảnh báo | 18/19 panel D1–D4 có dữ liệu | Ảnh chụp; D5 |

 

# **4\. Thu thập và dữ liệu**

## **4.1. Thay đổi ngày 12/11**

1\.     **Chỉ dùng API chính thức của Steam.** Đã xoá steamspy\_crawler.py, historical\_backfill.py, public\_datasets.py, kaggle\_replay\_producer.py, cursor\_manager.py. Tag lấy từ GetItems, giờ chơi từ từng review, quy mô game dùng tổng review.

2\.     **Đổi cách chọn game.** Lần thử 30 game ngẫu nhiên: 13 game không có review, 11 game chưa phát hành, 5 game chiếm 89,7% review. Cách mới: nhóm top\_ccu \+ nhóm stratified (lọc đã phát hành, ≥ 20 review, chia đều 4 tầng).

3\.     **Crawl review tăng dần, không trùng** (common/review\_state.py).

4\.     **Thêm nguồn histogram**: số review theo ngày do Steam đếm, dùng cho B6 và B7.

5\.     **Thêm trường dữ liệu**: review có refunded, comment\_count, last\_played, giờ chơi trên Deck; appdetails có DLC, achievement, số ngôn ngữ.

6\.     **Vòng phản hồi mới**: thêm game đang lên trên bảng most-played. Bản cũ dựa vào B6 nên không bao giờ phát hiện được game ngoài danh sách. Đang tắt trong giai đoạn demo.

## **4.2. Số liệu hiện có**

| Hạng mục | Số liệu | Giải thích |
| :---- | :---- | :---- |
| Game theo dõi | 100 | 20 top\_ccu \+ 80 stratified (20 mỗi tầng) |
| Game có review | 100 | Lần thử trước: 17/30 |
| Review raw | 92.019 | 92.019 ID khác nhau, 0 trùng qua 2 lượt crawl |
| Review tiếng Anh | 39.337 | Chỉ review tiếng Anh được phân tích nội dung |
| Khoảng thời gian review | 10/09/2013 – 12/11/2026 | 28.414 review trong 7 ngày gần nhất, thuộc 61 game |
| Mức độ tập trung | Game lớn nhất 2,2%; 5 game lớn nhất 10,6% | Lần thử trước: 5 game chiếm 89,7% |
| Metadata | 100 appdetails \+ 100 store \+ 100 histogram | Mỗi game đủ 3 loại |
| Review theo ngày (histogram) | 2.950 dòng | 30 ngày gần nhất mỗi game |
| Snapshot CCU | 200 | 2 lần thu (3h và 5h UTC ngày 12/11) |
| Review sau làm sạch | 87.407 | Lọc review rỗng/spam; giữ review ngôn ngữ khác để dùng nhãn |
| Tỷ lệ tích cực | 80,03% | Của mẫu 100 game, không đại diện toàn Steam |
| Dung lượng raw / lake | 18,3 MiB / 165,8 MiB |   |

 

## **4.3. Hạn chế của dữ liệu**

•      **Quy mô demo** nhỏ hơn thiết kế; có chủ đích để demo, mở rộng bằng build\_targets \--limit lớn hơn.

•      **CCU mới có 2 snapshot mỗi game**: chưa có xu hướng tuần; S3 chỉ có baseline 2 khung giờ.

•      **Review game lớn chỉ phủ gần đây**: mỗi lượt tối đa 10 trang (1.000 review) mỗi game.

•      **Mẫu không đại diện toàn catalog**: đã loại game \< 20 review và chưa phát hành.

•      **Giá mới có 1 snapshot**: chưa quan sát được đợt giảm giá nào bắt đầu, nên bảng tác động giảm giá trống.

# **5\. Batch layer B1–B8**

Lệnh: python pipeline.py \--profile local batch \--raw data/raw \--lake data/lake \--es, Spark local\[4\], driver 2–3 GB. **Kết thúc thành công**, khoảng 12 phút, B5 chậm nhất.

| Job | Nội dung | Kết quả trên dữ liệu thật | Trạng thái |
| :---- | :---- | :---- | :---- |
| B1 Ingest | Đọc Kafka theo offset, ghi JSONL gzip | Chạy với Kafka local, ghi data/kafka\_demo\_raw | Một phần |
| B2 Clean | Khử trùng, làm sạch, tách tag và số review/ngày, loại sự kiện demo, đánh dấu ngày đếm dở | 87.407 review; 200 CCU; 100 game; 2.950 dòng review/ngày | Xong |
| B5 Cảm xúc | VADER, khía cạnh, TF-IDF | 34.725 review; 6.266 dòng khía cạnh; 2.812 từ khoá | Xong |
| B3 Thống kê | Ngày, tuần/tháng, phân khúc, ngôn ngữ | 19.861 game/ngày; 9.565 game/kỳ; 4.780 phân khúc; 14.701 ngôn ngữ/ngày | Xong |
| B4 Giờ chơi | 5 nhóm giờ chơi, chê sớm, đáng tiền | 433 dòng; 100 hồ sơ game | Xong |
| B6 Trending | 7 ngày so với 7 ngày trước; z-score; giảm giá | 21.742 dòng; ROB RUN 21.899 review/7 ngày so với 637 | Xong (CCU còn mỏng) |
| B7 Baseline | Review/giờ cho S2, CCU cùng giờ cho S3 | 100 baseline review; 200 baseline CCU | Xong (CCU 2 khung giờ) |
| B8 Phân cụm | K-Means 6 cụm | 100 game; cỡ cụm 7, 53, 3, 10, 1, 26 | Xong |

 

## **5.1. Thay đổi logic (phát hiện khi chạy dữ liệu thật)**

•      **B6 đếm review từ histogram** thay vì review đã crawl, để không đánh giá thấp game lớn. Xếp hạng tại ngày đủ dữ liệu cuối cùng; game \< 10 review/tuần xếp cuối.

•      **B6 tác động giảm giá**: chỉ tính "bắt đầu sale" khi thấy bước chuyển không giảm → giảm. Bản cũ có 722 dòng sai do coi game đang giảm ở snapshot đầu là bắt đầu sale.

•      **B7 baseline review** dùng 7 ngày đủ dữ liệu của histogram; dưới 30 review thì % tích cực lấy theo tỷ lệ trọn đời của Steam. Loại được báo động giả ở game ít review.

## **5.2. Kết quả phân tích ban đầu**

•      **Trending**: ROB RUN (game mới) tăng 3.338% số review; Probably Stolen tăng 67%; Brotato tăng 33%.

•      **Mức hài lòng**: 80,03% review sạch là tích cực (phụ thuộc cách chọn mẫu).

•      **Khía cạnh, giờ chơi, phân cụm**: đã có bảng, cần đọc mẫu review và hồ sơ cụm trước khi kết luận. Cụm chỉ có 1 game cần xem lại đặc trưng.

# **6\. Streaming và hợp nhất Lambda**

Môi trường: Kafka 1 broker, Elasticsearch và Kibana bằng Docker Compose; S1–S3 chạy Spark local, đọc Kafka, ghi ES. Lệnh bash scripts/demo.sh stream-start.

| Dữ liệu đưa vào Kafka | Kết quả |
| :---- | :---- |
| CCU thật của 100 game, gọi Steam trực tiếp | S3 ghi CCU hiện tại vào realtime\_game\_metrics |
| 3.501 review thật trong 24 giờ qua | S1 ghi số review và % tích cực theo cửa sổ 5 và 60 phút; S2 **không** báo động giả |
| Bơm có nhãn: 80 review tiêu cực cho Counter-Strike 2 | S2 cảnh báo REVIEW\_BOMBING: 44 review tiêu cực / 60 phút, vượt ngưỡng 28,8; % tích cực 48,2% so với baseline 81,9% |
| Bơm có nhãn: CCU gấp 3 baseline cùng khung giờ | S3 cảnh báo CCU\_SPIKE, change\_ratio \= 2,0 |

 

•      S2 còn bắt được một cụm review tiêu cực **thật** của Overwatch: 10 review tiêu cực / giờ so với mức thường 2,4 ± 1,6; % tích cực 23% so với 52%. Đây là dấu hiệu cần điều tra, **chưa phải kết luận** có bombing.

•      Sự kiện bơm mang source=demo\_injection; B2 loại chúng khỏi dữ liệu phân tích.

•      merge\_views xoá 4.576 cửa sổ realtime của hôm qua (batch đã bao phủ) và giữ 2.690 cửa sổ của hôm nay.

•      Chưa đo độ trễ end-to-end; mới quan sát cảnh báo xuất hiện khoảng 1 phút với trigger 20 giây.

# **7\. Lưu trữ, truy vấn và dashboard**

## **7.1. Số document trong Elasticsearch**

| Index | Số document | Index | Số document |
| :---- | :---- | :---- | :---- |
| batch\_game\_daily | 19.861 | batch\_segment\_stats | 4.780 |
| batch\_trending | 21.742 | reviews\_search | 34.725 |
| batch\_language\_stats | 14.701 | game\_keywords | 2.812 |
| batch\_game\_period | 9.565 | batch\_playtime\_buckets | 433 |
| batch\_aspect\_stats | 6.266 | batch\_game\_retention, batch\_game\_clusters | 100 mỗi index |

 

realtime\_game\_metrics và alerts thay đổi theo phiên streaming. Hive Metastore chưa triển khai; truy vấn ad-hoc dùng Spark SQL trên Parquet.

## **7.2. Dashboard**

Cách kiểm tra: chạy chính truy vấn ES của từng panel Vega với khoảng thời gian mặc định rồi đếm kết quả. Cách này xác nhận panel có dữ liệu nhưng **chưa thay cho ảnh chụp dashboard**.

| Dashboard | Kết quả | Ghi chú |
| :---- | :---- | :---- |
| D1 Tổng quan | 9/9 panel có dữ liệu | KPI số game là cardinality ước lượng |
| D2 Chi tiết game | 5/5 panel | Lọc appid:\<id\>; chưa có annotation sự kiện trên timeline |
| D3 Realtime | 3/3 panel khi streaming chạy | Khoảng mặc định 24 giờ |
| D4 Cảnh báo, xu hướng | Cảnh báo, trending có dữ liệu; tác động giảm giá trống | Trống đúng: chưa có 2 snapshot giá thấy bước chuyển |
| D5 Hệ thống (Grafana) | Chưa chạy | Cần Prometheus trên cụm |

 

# **8\. Lỗi đã sửa trong ngày**

| Lỗi | Hậu quả | Cách sửa |
| :---- | :---- | :---- |
| Thời gian ghi ES lệch theo múi giờ máy | Realtime lệch 7 giờ trên máy giờ Việt Nam, D3/D4 trống | Luôn ghi UTC có múi giờ (es\_writer.\_jsonable) |
| Cutoff hợp nhất chỉ ghi ngày | ES làm tròn lên cuối ngày, xoá nhầm realtime của hôm nay | Cutoff là thời điểm chính xác, tối đa 00:00 UTC hôm nay |
| Kafka trong compose không khởi động | Không demo được streaming | Sửa quyền ghi volume |
| Báo động giả ở game ít review | S2 cảnh báo sai | Baseline từ histogram; \< 30 review dùng tỷ lệ trọn đời |
| B6 coi snapshot giảm giá đầu tiên là bắt đầu sale | 722 dòng tác động giảm giá sai | Chỉ tính khi thấy bước chuyển |
| Trending đếm từ review đã crawl | Đánh giá thấp game lớn | Đếm bằng histogram của Steam |
| Vòng phản hồi dựa vào B6 | Không phát hiện được game mới | Lấy tín hiệu từ bảng most-played của Steam |

 

Ngoài ra các vấn đề của checkpoint 11/11 đã khắc phục: dữ liệu lệch (1 game chiếm 43,5% review), 4 unit test cần Spark không chạy được (nay 36/36 đạt), chưa có thư mục tests/results/ (nay có CSV của SC1, SC3, SC4).

# **9\. Hạ tầng Kubernetes**

•      Có manifest: Kafka (Strimzi, KRaft, 3 broker, RF=3, min ISR=2), HDFS, ES (ECK) \+ Kibana, Spark CronJob batch và 3 Deployment streaming, CronJob crawler; bộ thu gọn k8s/demo và k8s/fault-test.

•      Đã chốt: 4 VM e2-standard-2 trên Google Cloud, k3s, Tailscale, chỉ bật theo buổi.

•      **Chưa chạy trên cụm thật.**

•      Trước khi lên cụm: build lại image (tag k3s-20261026 chưa có code 12/11), thêm CronJob store/histogram và vòng phản hồi, chép danh sách game và state crawler lên PVC.

# **10\. Kiểm thử**

## **10.1. Unit test: 36/36 đạt**

•      Thu thập: chọn game phân tầng, mở rộng danh sách không trùng, crawl tăng dần (30 kịch bản ngẫu nhiên; bản kiểm tra riêng 13.000 kịch bản).

•      Xử lý: trending và baseline từ histogram, loại sự kiện demo ở B2, luật cảnh báo S2/S3.

•      Serving: ghi thời gian UTC vào ES, cutoff khi hợp nhất.

•      Khác: tiền xử lý văn bản, khía cạnh, vòng phản hồi.

## **10.2. Scalability**

| Kịch bản | Trạng thái | Kết quả chạy thử một máy |
| :---- | :---- | :---- |
| SC1 Tăng executor | Chạy thử | 11.637 review: B2–B5 mất 74,8 giây (1 core) và 73,9 giây (4 core). Dữ liệu quá nhỏ, thời gian khởi động Spark chiếm phần lớn nên chưa thấy hiệu quả |
| SC2 Tăng dữ liệu | Chưa chạy | Script hỗ trợ 1×/2×/5×/10× |
| SC3 Tải streaming | Chạy thử | 200 và 1.000 review/giây: S1 xử lý 1.180 và 5.925 dòng/giây; micro-batch trung bình 1,4 và 1,3 giây (p95 2,5 và 1,9 giây); không tụt offset |
| SC4 Truy vấn ES | Chạy thử 1 node | 4 luồng: 303 truy vấn/giây, p95 64 ms; 16 luồng: 474 truy vấn/giây, p95 66 ms; 0 lỗi |

 

Lưu ý: thời gian micro-batch và offset lag **không phải** độ trễ end-to-end. Với dữ liệu phát lại \--retime, không lấy chênh lệch thời gian gốc của review làm độ trễ.

## **10.3. Fault-tolerance**

| Kịch bản | Trạng thái |
| :---- | :---- |
| FT1 Mất Kafka broker | Có script và observer kiểm tra bản ghi đã ACK; cần cụm |
| FT2 Mất DataNode | Có kịch bản; cần cụm, kiểm tra đủ bản sao trước khi gây lỗi |
| FT3 Mất executor/driver | Có kịch bản; cần cụm, đối soát ID sau khôi phục |
| FT4 Mất node ES | Cần ES nhiều node và replica |
| FT5 Mất worker | Cần cụm; nên tắt VM thật thay vì chỉ drain |
| FT6 API lỗi 429 | Đạt: crawler giãn nhịp, không dừng; lượt 1 thu 5/8 game, lượt 2 đủ 8/8 |

 

# **11\. Khó khăn**

| Khó khăn | Mức độ | Ảnh hưởng | Hướng xử lý |
| :---- | :---- | :---- | :---- |
| Chưa có bằng chứng trên cụm | Cao | Phần SC/FT, yêu cầu chính của đề, chưa chứng minh | Dựng cụm theo buổi; lưu log/CSV/ảnh/video |
| Không có lịch sử CCU | Cao | Chưa có xu hướng CCU; máy tắt là mất dữ liệu | Thu liên tục ≥ 14 ngày trên máy bật 24/7, sau chuyển CronJob |
| Thiếu RAM máy cá nhân (\~8 GB) | Trung bình | Không bật đồng thời mọi thành phần | Bật theo nhóm; dừng streaming khi chạy batch |
| Quy mô demo nhỏ | Trung bình | Có thể bị đánh giá chưa đủ big data; SC1 chưa thấy hiệu quả | Mở rộng \~1.000 game; hỏi giảng viên |
| Cấu hình thu gọn | Trung bình | FT4/SC4 không làm được với ES 1 node | Tạm nâng ES khi test; ghi rõ trong báo cáo |
| Quá nhiều file nhỏ | Trung bình | 1.020 file raw review, 10.737 file Parquet | Gộp raw theo ngày; B2 repartition theo dt |
| Giới hạn tốc độ Steam | Thấp | 1.000 game mất 4–6 giờ lượt đầu | Crawl tăng dần; chia worker trên cụm |
| Image và manifest lệch nhau | Thấp | Dễ chạy nhầm code cũ | Quy ước tag theo ngày; một cấu hình mỗi đợt test |

 

# **12\. Câu hỏi cần trao đổi với giảng viên**

Danh sách gộp từ decisions.md, van\_de\_va\_giai\_phap\_ha\_tang.md, BAO\_CAO\_TIEN\_DO.md và checkpoint docx:

1\.     Bỏ Hive Metastore, dùng Spark SQL đọc trực tiếp Parquet có đáp ứng yêu cầu lớp truy vấn (F5) không?

2\.     HDFS replication 2 và Elasticsearch 1 node / 0 replica có chấp nhận được không? Cấu hình nào bắt buộc nâng trước khi nghiệm thu FT4/SC4?

3\.     "No docker": dùng Docker Compose để phát triển và demo một máy, nghiệm thu trên k3s (containerd) có được không?

4\.     Cụm k3s 4 VM cloud nối bằng Tailscale có được coi là "mô phỏng môi trường phân tán" không?

5\.     Bộ demo 100 game / 92 nghìn review và mục tiêu khoảng 1.000 game, chỉ từ API Steam, có đủ quy mô không? Mức nào là đủ?

6\.     FT5 cần tắt hẳn VM hay chấp nhận drain node? Tiêu chí thời gian phục hồi cụ thể là gì?

7\.     Có được dùng MinIO (S3) thay HDFS nếu HDFS trên K8s gặp khó không?

# **13\. Kịch bản trình bày 8–10 phút**

1\.     **Mục tiêu và kiến trúc (1 phút):** 5 câu hỏi nghiệp vụ, luồng Lambda.

2\.     **Dữ liệu (1–2 phút):** vì sao chỉ dùng Steam; cách chọn 100 game; crawl tăng dần không trùng.

3\.     **Batch và dashboard (2–3 phút):** D1, D2 với appid:730, D4 trending (ROB RUN).

4\.     **Streaming trực tiếp (2 phút):** crawler đẩy CCU và review thật vào Kafka → D3 cập nhật. Bơm tình huống bombing → D4 có cảnh báo; nói rõ đây là dữ liệu bơm có nhãn và B2 loại khỏi batch.

5\.     **Khoảng trống và kế hoạch (2 phút):** cụm, SC/FT, lịch sử CCU, độ trễ.

Các lệnh demo nằm ở README mục 4.11.

| Đoạn phát biểu có thể dùng Thưa cô/thầy, đến ngày 12/11, nhóm đã chạy thông toàn bộ hệ thống Lambda trên dữ liệu thật: crawler lấy dữ liệu trực tiếp từ API Steam, Kafka nhận luồng sự kiện, Spark chạy 8 job batch và 3 job streaming, kết quả đưa lên Elasticsearch và Kibana. Nhóm đã bỏ bộ dữ liệu trộn nguồn trước đây và chỉ dùng API chính thức của Steam: 100 game được chọn theo phương pháp phân tầng, hơn 92 nghìn review không trùng. Cơ chế thu thập cho phép mở rộng mà không lấy lại dữ liệu cũ. Phần nhóm cần làm tiếp là thu dữ liệu liên tục để có lịch sử CCU, mở rộng quy mô, chạy trên cụm Kubernetes nhiều node và đo khả năng mở rộng, khả năng phục hồi và độ trễ streaming. |
| :---- |

 

**STEAM PLAYER INSIGHTS**

**Nhiệm vụ còn lại**

**Tính từ:** 12/11/2026 (sau checkpoint tiến độ)

**Mức ưu tiên:** P0 \= bắt buộc, thiếu là mất điểm lớn · P1 \= cần để nghiệm thu đầy đủ · P2 \= hoàn thiện bàn giao

**Lưu ý:** Lộ trình theo tuần là đề xuất, nhóm điều chỉnh theo hạn nộp thực tế

 

# **1\. Tổng quan nhiệm vụ**

| \# | Ưu tiên | Nhiệm vụ | Vai trò phụ trách | Xong khi |
| :---- | :---- | :---- | :---- | :---- |
| T1 | **P0** | Hỏi giảng viên chốt phạm vi | Cả nhóm (trưởng nhóm hỏi) | Có câu trả lời, ghi vào docs/decisions.md |
| T2 | **P0** | Bật vòng thu liên tục trên máy bật 24/7 | Dữ liệu | ≥ 14 ngày CCU; baseline S3 đủ 24 khung giờ |
| T3 | **P0** | Gộp file nhỏ ở raw và clean | Dữ liệu \+ Batch | Số file theo ngày thay vì theo game/nguồn |
| T4 | **P0** | Mở rộng danh sách lên \~1.000 game | Dữ liệu | Crawl tăng dần không trùng; báo cáo độ phủ từng tầng |
| T5 | **P0** | Chuẩn bị lên cụm: image, manifest, CronJob | Hạ tầng | Image tag mới; manifest B1–B8, S1–S3; CronJob crawler mới |
| T6 | **P0** | Dựng cụm k3s 4 VM, chạy toàn trình | Hạ tầng (+ cả nhóm hỗ trợ) | Log nodes, pods \-o wide; số document ES; ảnh D1–D4 |
| T7 | **P1** | SC1–SC4 trên cụm | Hạ tầng & kiểm thử | CSV trong tests/results/ \+ biểu đồ |
| T8 | **P1** | FT1–FT5 trên cụm | Hạ tầng & kiểm thử | Log trước/trong/sau lỗi, thời gian phục hồi, đối soát, video |
| T9 | **P1** | Đo độ trễ streaming end-to-end | Streaming & serving | Số đo so với yêu cầu ≤ 2 phút |
| T10 | **P1** | Chạy notebook, trả lời 5 câu hỏi nghiệp vụ | Batch & phân tích | Mỗi câu có số liệu, khoảng thời gian, cỡ mẫu, hạn chế |
| T11 | **P1** | Monitoring (Prometheus \+ Grafana D5) hoặc phương án thay | Dashboard & báo cáo \+ Hạ tầng | D5 có số liệu hoặc ghi rõ cách thay |
| T12 | **P1** | Sửa các điểm code cần rà lại | Theo từng mục | Mục 4 tài liệu này đã đánh dấu xong |
| T13 | **P2** | Bật vòng phản hồi gần ngày bảo vệ | Dữ liệu | Có game nhóm feedback được thêm và crawl |
| T14 | **P2** | Thống nhất lại tài liệu trong docs/ | Dashboard & báo cáo | Các điểm ở mục 5 đã sửa |
| T15 | **P2** | Báo cáo cuối kỳ, slide, video | Cả nhóm | Người ngoài nhóm chạy lại được theo tài liệu |

 

## **1.1. Phụ thuộc giữa các nhiệm vụ**

T1 (hỏi giảng viên) ──\> quyết định cấu hình cho T5, T6, T7, T8  
T2 (thu liên tục) ───\> cần ≥ 14 ngày ──\> T10 (phân tích CCU), chạy lại B6/B7  
T3 (gộp file nhỏ) ───\> T4 (mở rộng 1.000 game) ──\> SC1, SC2 có dữ liệu đủ lớn  
T5 (image, manifest) ─\> T6 (dựng cụm, toàn trình) ──\> T7, T8, T9, T11  
T6..T10 ─────────────\> T15 (báo cáo, slide, video)

| Việc phải bắt đầu ngay hôm nay T2 (vòng thu CCU) là việc duy nhất không thể làm bù: Steam không có API lịch sử CCU, mỗi ngày chậm là mất một ngày dữ liệu. Các việc khác có thể dồn, việc này thì không. |
| :---- |

 

# **2\. Chi tiết từng nhiệm vụ**

## **T1. Hỏi giảng viên chốt phạm vi (P0)**

**Mục tiêu:** biết trước những gì được chấp nhận để không làm thừa hoặc thiếu. Câu trả lời ảnh hưởng trực tiếp tới cấu hình cụm và kịch bản kiểm thử.

1\.     Bỏ Hive Metastore, dùng Spark SQL đọc Parquet có đáp ứng lớp truy vấn không?

2\.     HDFS replication 2, ES 1 node / 0 replica có được không? Cấu hình nào phải nâng trước nghiệm thu FT4/SC4?

3\.     Docker Compose chỉ để phát triển/demo một máy, nghiệm thu trên k3s (containerd) có vi phạm "No docker" không?

4\.     Cụm k3s 4 VM cloud nối Tailscale có được coi là mô phỏng phân tán không?

5\.     100 game / 92 nghìn review, mục tiêu \~1.000 game, chỉ từ Steam: mức nào là đủ?

6\.     FT5 cần tắt hẳn VM hay drain node là đủ? Tiêu chí thời gian phục hồi?

7\.     Có được dùng MinIO thay HDFS nếu HDFS trên K8s bế tắc không?

**Xong khi:** mỗi câu có câu trả lời, ghi vào docs/decisions.md mục 5 kèm ngày.

## **T2. Bật vòng thu liên tục (P0)**

**Mục tiêu:** tích luỹ lịch sử CCU, review mới và giá để B6 tính được xu hướng CCU, S3 có baseline đủ 24 khung giờ và D4 quan sát được đợt giảm giá.

**Các bước:**

1\.     Chọn **một** máy bật 24/7 (máy cá nhân không ngủ, hoặc một VM nhỏ). Từ lúc này chỉ máy này được crawl.

2\.     Kiểm tra .env có STEAM\_API\_KEY, CURSOR\_STATE\_DIR=data/crawler\_state.

3\.     Chạy vòng lặp có sẵn ở README mục 4.3 (A6), gồm ba lịch:

\# CCU: 10–15 phút/lượt, liên tục ≥ 14 ngày  
python \-u \-B \-m crawlers.collect\_local \--sources ccu \--max-requests 300  
\# Review mới: 30–60 phút/lượt  
python \-u \-B \-m crawlers.collect\_local \--sources reviews \--max-requests 1500  
\# Store, metadata (giá), histogram: hằng ngày  
python \-u \-B \-m crawlers.collect\_local \--sources store metadata histogram \--max-requests 500

1\.     Mỗi ngày kiểm tra data/raw/collection-worker-0.json: xem failures, failed\_sources.

2\.     Định kỳ (ví dụ mỗi 3 ngày) đóng gói dữ liệu lên Drive cho cả nhóm (README 4.3, A7). **Dừng vòng thu trước khi đóng gói.**

| Lưu ý •      Dùng cùng \--state-dir cho mọi lượt, chạy tuần tự, không chồng tiến trình. •      Máy tắt hoặc ngủ là mất CCU khoảng đó, không lấy lại được. Ghi lại các khoảng bị mất để nêu trong báo cáo. •      Khi có cụm: chuyển sang CronJob, mang theo data/crawler\_state và data/target\_apps.json, dừng vòng thu trên máy cá nhân. |
| :---- |

 

**Xong khi:** ≥ 14 ngày CCU liên tục; ccu\_baseline có đủ 24 khung giờ cho nhóm top\_ccu; có ít nhất 2 snapshot giá thấy được một đợt giảm giá.

## **T3. Gộp file nhỏ (P0, làm trước T4)**

**Vấn đề:** raw có 1.020 file review cho 92 nghìn review (CCU 1 bản ghi mỗi file); clean/reviews có 10.737 file Parquet do phân vùng theo ngày tạo review (2013–2026). Lên HDFS với 1.000 game, NameNode và Spark sẽ tốn rất nhiều thời gian liệt kê và mở file.

**Các bước:**

1\.     Viết script gộp raw: mỗi thư mục dt= của mỗi nguồn gộp thành một hoặc vài file .jsonl.gz. Giữ nguyên layout thư mục để B2 đọc như cũ.

2\.     Sửa crawler local: gom bản ghi theo nguồn trong bộ đệm và flush khi đủ khoảng 500 bản ghi hoặc 60 giây, **nhưng vẫn phải ghi xong file trước khi lưu state review** (để không phá cơ chế không trùng).

3\.     Sửa B2: repartition theo dt (hoặc coalesce) trước khi ghi Parquet; cân nhắc phân vùng clean/reviews theo tháng thay vì ngày cho review cũ.

4\.     Chạy lại batch, so số dòng trước/sau (phải bằng nhau), so thời gian chạy.

**Xong khi:** số file raw tính theo ngày × nguồn; số file Parquet clean giảm rõ rệt; unit test vẫn 36/36; số review clean không đổi.

## **T4. Mở rộng danh sách lên \~1.000 game (P0, sau buổi báo cáo tiến độ)**

python \-B \-m crawlers.build\_targets \--limit 1000 \--top-ccu 50 \--seed 43

•      Game cũ giữ nguyên ở đầu danh sách; game mới không trùng; manifest thêm một mục trong batches.

•      Lượt crawl đầu mất khoảng 4–6 giờ. Nên chia nhiều lượt bằng \--max-requests và \--start-index (lấy từ next\_game\_index trong báo cáo).

•      Kiểm tra tầng nào bị thiếu (topped\_up trong manifest) và báo cáo độ phủ từng tầng.

•      Khi danh sách lớn, một vòng CCU sẽ dài hơn 10 phút. Ưu tiên CCU cho nhóm top\_ccu, hoặc chia worker khi lên cụm.

**Xong khi:** danh sách \~1.000 game, crawl xong lượt đầu, không có review trùng (kiểm bằng B2), có bảng độ phủ theo tầng.

## **T5. Chuẩn bị lên cụm (P0, làm trên máy cá nhân trước khi bật cloud)**

**Mục tiêu:** mọi thứ sẵn sàng để thời gian bật VM (tốn tiền) chỉ dùng để chạy và đo.

1\.     **Build lại image** Spark và crawler, tăng tag dạng k3s-YYYYMMDD, sửa tag ở **mọi** manifest (tag cũ k3s-20261026 chưa có code 12/11).

2\.     **Viết manifest Job/Deployment** cho B1–B8 và S1–S3 trong bộ thu gọn (hiện mới có B2 và smoke test).

3\.     **Thêm CronJob** cho collect\_local \--sources store metadata histogram (hằng ngày) và update\_targets\_from\_trending (để sẵn, bật gần ngày bảo vệ). k8s/crawlers/crawler-cronjobs.yaml hiện mới có CCU, review, appdetails, AppList.

4\.     Thêm S2\_MIN\_NEGATIVE vào ConfigMap steam-pipeline-config (hiện chỉ có S2\_SIGMA, S2\_POSITIVE\_DROP\_POINTS, S3\_CHANGE\_THRESHOLD).

5\.     Thêm spec.timeZone: "Etc/UTC" cho CronJob batch (lịch 01:30 ghi chú là UTC nhưng manifest chưa khai báo).

6\.     Chuẩn bị PVC crawler: data/target\_apps.json, manifest, data/crawler\_state/reviews\_v2/.

7\.     Chạy kubeconform lại cho toàn bộ manifest.

8\.     Viết sẵn danh sách lệnh cần chạy và cần chụp cho buổi đầu (mục 3.1).

| Kiểm tra sức chứa CPU trước khi bật cloud Cụm có 8 vCPU (đúng bằng giới hạn Free Trial), nên không thêm VM được. Bộ triển khai đầy đủ cần 12–16 CPU, vì vậy chỉ chạy bộ thu gọn k8s/fault-test. Cần cộng tổng requests.cpu của mọi pod (Kafka 3 broker, HDFS, ES, Kibana, Spark driver \+ executor, crawler, hệ thống k3s) để chắc chắn còn chỗ, nhất là khi phải tạm nâng ES lên 2–3 node cho FT4/SC4. |
| :---- |

 

## **T6. Dựng cụm k3s 4 VM và chạy toàn trình (P0)**

1\.     Tạo project GCP, **đặt Budget alert ngay**. Tạo 4 VM e2-standard-2, Ubuntu 24.04.

2\.     Cài Tailscale và k3s theo infra/k3s/README.md (1 server \+ 3 agent).

3\.     Nạp image vào các node, triển khai bộ k8s/fault-test.

4\.     Nạp dữ liệu: chép raw lên HDFS (hdfs dfs \-put) hoặc replay qua Kafka (không dùng \--retime).

5\.     Chạy toàn trình: crawler → Kafka → B1 → HDFS → B2–B8 → ES, cùng S1–S3; chạy tay batch bằng kubectl create job batch-now \-n spark-jobs \--from=cronjob/steam-batch-daily.

6\.     Dựng index và dashboard Kibana, chụp D1–D4.

7\.     Lưu bằng chứng (mục 3.2), **stop VM** khi xong buổi.

**Xong khi:** có kubectl get nodes, kubectl get pods \-A \-o wide cho thấy pod rải trên các node, số document từng index ES, ảnh D1–D4, tất cả lưu trong thư mục có ngày và đã commit.

## **T7. Kiểm thử scalability SC1–SC4 (P1)**

| Kịch bản | Cách làm | Số đo cần có |
| :---- | :---- | :---- |
| SC1 Tăng executor | Chạy B2–B5 với 1 → 2 → 4 → 6 executor trên dữ liệu đủ lớn (sau T4, hoặc nhân dữ liệu) | Thời gian chạy, speedup so với 1 executor |
| SC2 Tăng dữ liệu | Chạy batch với dữ liệu 1×, 2×, 5×, 10× (ghi rõ phần dữ liệu sinh thêm) | Thời gian theo kích thước dữ liệu |
| SC3 Tải streaming | Replay \--retime ở nhiều tốc độ; tăng partition Kafka và executor | Throughput, thời gian micro-batch (p50/p95), consumer lag |
| SC4 Tải truy vấn ES | Bắn truy vấn 4/16 luồng với ES 2 node so với 3 node | Truy vấn/giây, p50/p95, lỗi, thời gian về trạng thái green |

 

**Xong khi:** CSV trong tests/results/, biểu đồ, ghi kèm topology, image tag và tham số của từng lần chạy.

## **T8. Kiểm thử fault-tolerance FT1–FT5 (P1)**

| Kịch bản | Gây lỗi | Chứng minh cần có |
| :---- | :---- | :---- |
| FT1 Mất Kafka broker | Xoá pod 1 broker khi crawler đang ghi | Producer vẫn ghi được; mọi bản ghi đã ACK đều còn (observer) |
| FT2 Mất DataNode | Xoá pod 1 DataNode khi batch đang đọc | Kiểm tra đủ bản sao trước; job vẫn xong; HDFS tự nhân bản lại |
| FT3 Mất executor / driver | Xoá pod executor khi batch chạy; xoá driver streaming | Batch tự chạy lại task; streaming đọc tiếp từ checkpoint; đối soát ID không trùng/thiếu |
| FT4 Mất node ES | Xoá 1 node ES (cần ≥ 2 node, có replica) | Dashboard vẫn truy vấn được; thời gian về green |
| FT5 Mất worker | Tắt hẳn 1 VM (nên tắt thật, không chỉ drain) | Pod được dời sang node khác; thời gian phục hồi |

 

**Xong khi:** mỗi kịch bản có log trước/trong/sau lỗi, thời gian phục hồi, đối soát dữ liệu và video. Kịch bản chi tiết ở infra/k3s/FAULT\_TEST.md.

## **T9. Đo độ trễ streaming end-to-end (P1)**

•      Định nghĩa: **thời điểm sự kiện được đưa vào Kafka → thời điểm kết quả xuất hiện trên Elasticsearch**. Không dùng thời gian micro-batch hay offset lag thay cho số này.

•      Cách làm gợi ý: demo\_inject.py ghi thêm thời điểm gửi vào bản ghi; S1/S2 ghi thời điểm ghi ES; một script truy vấn ES để tính hiệu.

•      Đo ở nhiều tốc độ tải, báo cáo p50/p95/max.

•      Không lấy chênh lệch thời gian gốc của review phát lại (\--retime) làm độ trễ.

**Xong khi:** có số đo so với yêu cầu ≤ 2 phút, cả trên một máy và trên cụm.

## **T10. Chạy notebook và trả lời 5 câu hỏi nghiệp vụ (P1)**

| Câu hỏi | Dữ liệu dùng | Cần lưu ý khi viết kết luận |
| :---- | :---- | :---- |
| 1\. Hài lòng thay đổi theo thời gian | game\_daily\_stats, game\_period\_stats, histogram | Review game lớn chỉ phủ gần đây; dùng histogram cho số đếm |
| 2\. Khía cạnh bị phàn nàn | aspect\_stats, keywords | Chỉ review tiếng Anh; đọc mẫu review để kiểm tra từ điển |
| 3\. Giờ chơi và đánh giá | playtime\_buckets, game\_retention | "Chê sớm" là chỉ báo gián tiếp |
| 4\. Bất thường | alerts, baseline | Phân biệt sự kiện bơm và sự kiện thật; Overwatch chỉ là dấu hiệu, chưa kết luận |
| 5\. Tăng trưởng, giảm giá | trending, event\_impact | Cần ≥ 14 ngày; giảm giá cần quan sát được bước chuyển |

 

**Xong khi:** 3 notebook đã chạy trên dữ liệu mới, có hình; mỗi câu trả lời có số liệu, khoảng thời gian, cỡ mẫu và hạn chế. Xem lại cụm K-Means chỉ có 1 game trước khi diễn giải.

## **T11. Monitoring (P1)**

•      Nếu tài nguyên cụm cho phép: cài kube-prometheus-stack, ES exporter, PodMonitor Kafka; import dashboards/grafana/d5\_system.json.

•      Nếu không: dùng kubectl top nodes/pods và Kafka consumer lag, ghi định kỳ ra CSV trong lúc test SC/FT, và ghi rõ phương án thay thế trong báo cáo.

## **T13. Bật vòng phản hồi (P2, gần ngày bảo vệ)**

python \-B \-m crawlers.update\_targets\_from\_trending \--top 5   \# hoặc: python pipeline.py feedback \-- \--top 5

Chạy hằng ngày (CronJob trên cụm). **Xong khi** có game nhóm feedback được thêm vào danh sách và đã có dữ liệu crawl.

## **T15. Bàn giao (P2)**

•      Báo cáo cuối kỳ: kiến trúc, dữ liệu, kết quả 5 câu hỏi, SC/FT, các điểm thu gọn so với thiết kế và lý do.

•      Slide và video demo (kịch bản ở README mục 4.11).

•      Tài liệu chạy lại: một người ngoài nhóm làm theo được từ đầu.

# **3\. Checklist cho mỗi buổi trên cloud**

## **3.1. Trước khi bật VM**

•      Image đã build với code mới nhất, tag mới đã có trong mọi manifest.

•      Manifest đã qua kubeconform.

•      Biết rõ buổi này chạy kịch bản nào, ai làm gì, tham số gì.

•      Có sẵn danh sách lệnh, script đo, thư mục lưu kết quả tests/results/\<ngày\>/.

•      Budget alert đang bật; đã kiểm tra số credit còn lại.

## **3.2. Trong buổi: bằng chứng cần lưu**

•      kubectl get nodes \-o wide và kubectl get pods \-A \-o wide.

•      Image tag, topology (số broker, DataNode, node ES, executor) và tham số của từng lần chạy.

•      Số document từng index ES.

•      Log trước, trong và sau mỗi lần gây lỗi; thời điểm gây lỗi và thời điểm phục hồi.

•      CSV kết quả đo, ảnh dashboard, video màn hình.

## **3.3. Sau buổi**

•      **Stop toàn bộ VM** (kiểm tra lại trên console GCP).

•      Commit kết quả và log vào repo.

•      Ghi vào docs/PLAN.md: đã làm gì, còn gì, lỗi gặp phải.

# **4\. Điểm trong code cần rà lại (T12)**

| Điểm | Vấn đề | Việc cần làm |
| :---- | :---- | :---- |
| File nhỏ | Crawler flush sau mỗi game/nguồn; B2 ghi Parquet theo ngày tạo review mà chưa repartition | Xem T3 |
| Dashboard và serving\_state | D1/D2 đọc batch, D3 đọc realtime; chưa đọc serving\_state nên chưa liền mạch quanh cutoff | Cho panel đọc cutoff từ serving\_state |
| D2 | Chưa có annotation sự kiện trên timeline; CCU mới theo ngày, chưa theo giờ | Thêm annotation (giảm giá, cảnh báo); thêm biểu đồ CCU theo giờ khi có dữ liệu |
| CronJob batch | Lịch 01:30 ghi chú UTC nhưng thiếu spec.timeZone | Thêm timeZone: "Etc/UTC" |
| Ngưỡng S2 | Luật 3σ trên số đếm nhỏ thỉnh thoảng cảnh báo ở game nhỏ | Thống kê tỷ lệ cảnh báo theo thời gian khi có thêm dữ liệu, chỉnh S2\_SIGMA, S2\_MIN\_NEGATIVE |
| ConfigMap | Thiếu S2\_MIN\_NEGATIVE | Thêm vào steam-pipeline-config |
| Ngữ nghĩa xử lý | Hệ thống là at-least-once (B1 ghi raw rồi mới lưu offset) | Không tuyên bố exactly-once trong báo cáo |

 

# **5\. Thống nhất lại tài liệu trong docs/ (T14)**

Khi đọc chéo các tài liệu hiện có, có một số chỗ mâu thuẫn hoặc dễ gây hiểu nhầm cho thành viên mới:

| File | Điểm chưa khớp | Đề xuất sửa |
| :---- | :---- | :---- |
| architecture.md §5 | Bảng chịu lỗi ghi HDFS 3 DataNode/replication 3, ES 3 node/1 replica | Ghi rõ là thiết kế đầy đủ; thêm cột cấu hình thu gọn đang dùng |
| architecture.md §1, §6 | Nhắc bảng Hive steam\_analytics và HMS 3.1.3 như đang có | Ghi chú Hive chưa triển khai trong cấu hình thu gọn |
| architecture.md §6 | Nhắc "bảng §5.5b" mà không nói của tài liệu nào | Ghi rõ "§5.5b của BigData.pdf" |
| van\_de\_va\_giai\_phap\_ha\_tang.md, PLAN.md | Một nơi ghi nhóm 5 người, nơi kia phân công 4 người (A–D) | Thống nhất 5 vai trò như tài liệu này |
| Nhiều file | Danh sách câu hỏi cho giảng viên mỗi nơi một khác (4, 5, 6 câu) | Giữ một danh sách duy nhất trong decisions.md, các file khác dẫn link |
| local\_setup\_guide.md | Bảng ghi make b1…make b6 trong khi có B1–B8; đánh số mục bắt đầu từ 4.1 và trỏ "mục 5, mục 7 của README" | Sửa thành B1–B8; đánh số lại hoặc ghi rõ nguồn |
| deployment\_guide.md và decisions.md | Tên image khác nhau: steam-spark:1.0.0 và localhost/steam-spark-demo:k3s-... | Ghi rõ image nào dùng cho bộ đầy đủ, image nào cho bộ thu gọn |
| deployment\_guide.md §4 | Danh sách ngưỡng thiếu S2\_MIN\_NEGATIVE | Bổ sung |
| Checkpoint docx | Ghi "sửa 9 lỗi" nhưng báo cáo chi tiết chỉ liệt kê khoảng 7 | Liệt kê đủ hoặc sửa con số |

 

# **6\. Lộ trình đề xuất**

| Thời gian | Việc chính | Đầu ra |
| :---- | :---- | :---- |
| Ngay hôm nay | Bật vòng thu liên tục (T2); tải gói dữ liệu lên Drive; commit code | CCU bắt đầu tích luỹ; cả nhóm có cùng dữ liệu |
| Tuần 1 | Báo cáo tiến độ; hỏi giảng viên (T1); gộp file nhỏ (T3); mở rộng \~1.000 game (T4); bắt đầu T5 | Câu trả lời của giảng viên; danh sách mở rộng, crawl lượt đầu |
| Tuần 2 | Hoàn tất T5; dựng cụm k3s 4 VM, chạy toàn trình (T6) | Ảnh nodes/pods, dashboard, số document ES |
| Tuần 3 | SC1–SC4 (T7), FT1–FT5 (T8), đo độ trễ (T9), monitoring (T11) | CSV, biểu đồ, video từng kịch bản |
| Tuần 4 | Chạy lại batch với ≥ 14 ngày CCU; notebook và 5 câu hỏi (T10); bật vòng phản hồi (T13); sửa tài liệu (T14); báo cáo, slide, video (T15) | Bộ bàn giao cuối kỳ |

 

# **7\. Phân công gợi ý theo vai trò**

| Vai trò | Phụ trách | Nhiệm vụ chính |
| :---- | :---- | :---- |
| Dữ liệu | Vòng thu liên tục, mở rộng danh sách, gộp file nhỏ, kiểm tra độ phủ, vòng phản hồi | T2, T3, T4, T13 |
| Batch & phân tích | B2–B8, notebook, trả lời 5 câu hỏi nghiệp vụ, repartition ở B2 | T3, T10 |
| Streaming & serving | S1–S3, merge\_views, đo độ trễ, D3/D4, serving\_state trên dashboard | T9, T12 |
| Hạ tầng & kiểm thử | Image, manifest, CronJob, cụm k3s, SC1–SC4, FT1–FT5 | T5, T6, T7, T8 |
| Dashboard & báo cáo | Kibana/Grafana, ảnh chụp, thống nhất tài liệu, báo cáo, slide, video | T11, T14, T15 |

 

Buổi trên cloud (T6–T8) nên có ít nhất 2 người: một người thao tác, một người ghi thời điểm và quay màn hình. T1 và T15 cả nhóm cùng làm.

# **8\. Rủi ro và phương án dự phòng**

| Rủi ro | Phương án |
| :---- | :---- |
| Hết credit hoặc quên tắt VM | Bật theo buổi, Budget alert, checklist sau buổi; dự phòng Azure for Students hoặc ghép máy cá nhân qua Tailscale |
| Máy chạy vòng thu CCU bị tắt | Mất dữ liệu khoảng đó; chọn máy bật 24/7, ghi lại khoảng bị mất, chuyển sang CronJob khi có cụm |
| Steam giới hạn tốc độ khi mở rộng | Crawl tăng dần nhiều lượt; chia worker trên cụm; ưu tiên CCU |
| Thiếu CPU/RAM trên cụm | Chỉ chạy bộ thu gọn; dừng streaming khi chạy batch (kubectl scale \--replicas=0); giảm maxExecutors |
| HDFS trên K8s quá khó | Dùng manifest có sẵn; nếu bế tắc, đề xuất MinIO (cần giảng viên đồng ý) |
| Chạy nhầm image cũ trên cụm | Quy ước tag theo ngày; ghi tag cùng kết quả mỗi lần đo |
| Chậm tiến độ hạ tầng | Pipeline đã chạy toàn trình local; buổi cloud chỉ để triển khai và đo |

 

# **1\. Hiện trạng**

**Đã xong:** crawl thật 100 game (92.019 review, không trùng); B1–B8 và S1–S3 chạy được trên máy cá nhân và ghi Elasticsearch; dashboard D1–D4 có dữ liệu; unit test 36/36 đạt.

**Chưa làm:**

·        Chưa chạy trên cụm k3s thật.  
·        Chưa có vòng thu dữ liệu liên tục.  
·        SC1–SC4 và FT1–FT5 chưa chạy trên cụm.  
·        Quá nhiều file nhỏ: 10.737 file Parquet cho 92 nghìn review.  
·        Chưa có báo cáo cuối, slide, video demo.

# **2\. Nhiệm vụ thêm: so sánh 100 / 500 / 1000 game**

Sau buổi demo, cô yêu cầu so sánh hiệu suất và các chỉ số khác của hệ thống ở ba quy mô 100, 500 và 1000 game. Để so sánh công bằng, nhóm thống nhất cách làm như sau.

## **2.1. Bộ dữ liệu lồng nhau**

Danh sách game do **build\_targets** tạo chỉ thêm vào cuối, không xáo lại, nên 100 game đầu nằm trong 500 và 500 nằm trong 1000\. Mở rộng bằng **\--limit 500** rồi **\--limit 1000**, giữ nguyên seed.

## **2.2. Tách hai câu hỏi**

·        **Chi phí crawl:** đo thật ở từng mức khi mở rộng danh sách (thời gian, số request, lỗi 429).  
·        **Hiệu năng xử lý:** sau khi crawl đủ 1000 game, cắt một snapshot duy nhất thành 3 tập con theo appid (100, 500, 1000 game đầu danh sách). Ba tập cùng khoảng thời gian, nên khác biệt chỉ đến từ số game.

## **2.3. Lưu ý khi lên 1000 game**

100 game cần khoảng 1.300 request, mất khoảng 35 phút ở interval 1,5 giây. Ước tính 1000 game cần khoảng 13.000 request, mất 5–6 giờ cho lượt đầu. Riêng một lượt CCU cho 1000 game đã mất khoảng 25 phút, nên không giữ được chu kỳ CCU 10 phút như hiện nay. Cần đổi chu kỳ và ghi rõ trong báo cáo; đây cũng là một kết quả so sánh.

## **2.4. Quy ước đo**

·        Tất cả kết quả ghi vào CSV theo một mẫu chung, ví dụ cột: **scale, stage, job, executors, run, seconds, records, bytes, peak\_mem**.  
·        Mỗi phép đo chạy ít nhất 3 lần, lấy trung vị.  
·        Lưu kết quả vào **tests/results/**, đặt tên file có quy mô và ngày đo.

# **3\. Phân công**

## **Hải Long: Dữ liệu và crawl**

·        Bật vòng thu liên tục trên một máy chạy 24/7 (README mục 4.3). Chỉ một nơi crawl tại một thời điểm; khi chuyển máy phải mang theo data/crawler\_state.  
·        Mở rộng 100 → 500 → 1000 game; sau mỗi mức lưu báo cáo crawl.  
·        Viết script cắt snapshot thành 3 tập con theo appid (ví dụ tests/scalability/make\_scale\_subsets.py), ghi ra data/scale/raw\_100, raw\_500, raw\_1000.  
·        **Số đo nộp:** thời gian crawl, số request, request/giây, số review/game, tỷ lệ lỗi và 429, dung lượng raw, độ phủ (bao nhiêu game có review, CCU, histogram).

## **Mạnh Hùng: Batch layer và benchmark batch**

·        **Làm trước khi mở rộng:** sửa vấn đề file nhỏ. Gộp raw theo ngày và cho B2 repartition theo dt trước khi ghi (PLAN.md mục 3.1.5).  
·        Sửa tests/scalability/sc1\_sc2\_batch\_benchmark.py để đo đủ B2–B8 (hiện chỉ đo b2, b5, b3, b4) và chạy trên 3 tập thật qua \--raw-template.  
·        Chạy ma trận 3 tập × {1, 2, 4} executor: trước ở local, sau trên cụm cùng thành viên D.  
·        **Số đo nộp:** thời gian từng job, speedup, thời gian khởi động Spark so với thời gian xử lý, số file và dung lượng Parquet, RAM/CPU đỉnh.  
·        **Sau khi xong phần file nhỏ:** làm mô hình phân loại cảm xúc B9 (xem mục 6).

## **Mai Kiên: Streaming, Elasticsearch, dashboard**

·        Thêm đo độ trễ end-to-end (từ lúc nhập vào Kafka đến lúc ghi xong vào ES) cho S1–S3.  
·        Replay từng tập vào Kafka bằng replay\_jsonl\_to\_kafka; đo throughput, consumer lag, độ trễ p50/p95 (SC3).  
·        SC4: đo thời gian truy vấn ES p50/p95 trên các panel D1–D4 ở cả 3 mức; ghi số document và dung lượng index.  
·        Chụp ảnh dashboard ở cả 3 mức để đưa vào slide.

## **Nhật Trung: Cụm k3s và kiểm thử chịu lỗi**

·        Build lại image Spark và crawler (tag hiện là k3s-20261026), sửa tag trong mọi manifest. Thêm CronJob cho store/metadata/histogram và vòng phản hồi.  
·        Dựng 4 VM GCP theo infra/k3s/README.md; chạy toàn trình crawler → Kafka → B1 → HDFS → B2–B8 → ES, cùng S1–S3.  
·        Chạy FT1–FT5 theo infra/k3s/FAULT\_TEST.md, nên chạy ở mức 1000 game để thấy rõ thời gian hồi phục. Lưu CSV, log trước/trong/sau lỗi, video.  
·        Hỗ trợ B và C khi benchmark trên cụm. Ghi số giờ VM và chi phí cho từng mức. Tắt VM ngay sau mỗi buổi.

## **Trưởng nhóm: Tích hợp và tổng hợp**

·        Chốt mẫu CSV chung trước khi mọi người bắt đầu đo; review và ghép code.  
·        Chạy lại 3 notebook trên dữ liệu mới; thêm phần so sánh theo quy mô vào notebook 03\.  
·        So sánh chất lượng phân tích theo quy mô, ngoài hiệu năng:

·        Top trending có ổn định khi tăng số game không.  
·        Silhouette của K-Means (B8).  
·        Phân bố cảm xúc.  
·        Số cảnh báo S2/S3.  
·        5 câu hỏi nghiệp vụ có đổi kết luận không.

# **4\. Thứ tự và phụ thuộc**

| Giai đoạn | Việc | Điều kiện bắt đầu |
| :---- | :---- | :---- |
| 1 | Hải Long mở rộng lên 500 rồi 1000 game · Mạnh Hùng sửa file nhỏ, sửa benchmark · Mai Kiên thêm đo độ trễ · Nhật Trung build image, dựng cụm | Làm song song |
| 2 | Hải Long cắt snapshot thành 3 tập · Mạnh Hùng làm B9, benchmark ở local · Mai Kiên benchmark ở local · Nhật Trung chạy toàn trình trên cụm | Có đủ 1000 game; B9 cần B5 đã sửa file nhỏ |
| 3 | Mạnh Hùng và Mai Kiên benchmark trên cụm (gồm B9) · Nhật Trung chạy FT1–FT5 | Cụm đã ổn định |
| 4 | Trưởng nhóm tổng hợp notebook, báo cáo · cả nhóm làm slide, video | Có đủ CSV |

 

# **5\. Sản phẩm cho phần so sánh**

Bảng tổng hợp: mỗi mức 100 / 500 / 1000 là một cột, các dòng như sau.

| Nhóm chỉ số | Nội dung (người phụ trách) |
| :---- | :---- |
| Chi phí crawl | Thời gian, request, lỗi 429, độ phủ (Hải Long) |
| Lưu trữ | Dung lượng raw/Parquet, số file (Hải Long, Mạnh Hùng) |
| Batch | Thời gian B2–B8, speedup theo executor (Mạnh Hùng) |
| Streaming | Throughput, consumer lag, độ trễ p50/p95 (Mai Kiên) |
| Truy vấn ES | Thời gian truy vấn D1–D4, số document, dung lượng index (Mai Kiên) |
| Tài nguyên và chi phí | RAM/CPU đỉnh, giờ VM, chi phí GCP (Mạnh Hùng, Nhật Trung) |
| Chịu lỗi | Thời gian hồi phục FT1–FT5 (Nhật Trung) |
| Mô hình ML (B9) | Thời gian huấn luyện, accuracy/F1, so với VADER (Mạnh Hùng) |
| Chất lượng phân tích | Độ ổn định trending, silhouette, kết luận nghiệp vụ (trưởng nhóm) |

 

Kèm theo: biểu đồ thời gian theo quy mô, biểu đồ speedup theo số executor, và phần nhận xét: chỗ nào tăng tuyến tính, chỗ nào thời gian khởi động Spark chiếm phần lớn (đã thấy khi chạy một máy với 11.637 review), nút thắt nằm ở crawl hay ở xử lý.

# **6\. Đề xuất: thêm một mô hình ML (B9)**

Dự án đã có ML: VADER và TF-IDF ở B5, K-Means ở B8, ngưỡng 3-sigma ở S2/S3. Nhóm chỉ thêm một mô hình, vì nó phục vụ luôn bài so sánh 100 / 500 / 1000 game. Không thêm mô hình nào khác.

## **6.1. Nội dung**

·        **Mô hình:** Logistic Regression trên Spark MLlib, dự đoán review tích cực hay tiêu cực từ nội dung review.  
·        **Nhãn:** trường voted\_up có sẵn trong review, không phải gán nhãn tay.  
·        **Đặc trưng:** dùng lại RegexTokenizer, StopWordsRemover, CountVectorizer, IDF đã có ở B5.  
·        **Chia dữ liệu:** train/test 80/20 với seed cố định. Nên chia theo thời gian (review cũ để train, review mới để test) cho sát thực tế.  
·        **Mất cân bằng nhãn:** review tích cực thường chiếm đa số, nên báo cáo F1 và AUC, không chỉ accuracy; cân nhắc dùng weightCol.  
·        **Vị trí:** job mới processing/batch/b9\_sentiment\_classifier.py, chạy sau B5; thêm vào pipeline.py và Makefile.

## **6.2. Kết quả cần nộp**

·        Accuracy, F1, AUC của mô hình trên tập test, so sánh với VADER trên cùng tập test (VADER: compound ≥ 0,05 là tích cực).  
·        Thời gian huấn luyện và dự đoán ở 3 mức 100 / 500 / 1000 game, với 1, 2, 4 executor.  
·        Đường cong học: độ chính xác thay đổi thế nào khi tăng dữ liệu từ 100 lên 1000 game.  
·        Các từ có trọng số dương/âm lớn nhất, đối chiếu với từ khoá tiêu cực của B5.

Ghi kết quả vào mẫu chung tests/results/common/ với experiment=ml, job b9 và vader, metric train\_s, predict\_s, accuracy, f1, auc (tests/results/README.md).

## **6.3. Phạm vi**

·        **Người làm:** Mạnh Hùng, khoảng 2–3 ngày, sau khi xong phần sửa file nhỏ.  
·        **Không làm:** BERT hay deep learning (không đủ RAM, không thể hiện được tính phân tán); dự đoán CCU (chưa đủ vài tuần dữ liệu liên tục); hệ gợi ý game (lệch khỏi 5 câu hỏi nghiệp vụ).  
·        **Ưu tiên:** nếu trễ tiến độ, B9 là việc bỏ đầu tiên. Cụm, benchmark và FT luôn được ưu tiên hơn.

[image1]: <data:image/png;base64,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>