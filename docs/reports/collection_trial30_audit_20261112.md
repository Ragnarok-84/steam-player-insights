# Kiểm tra dữ liệu crawl thử 30 game — 12/11/2026

Kết luận: dữ liệu raw đủ tốt để kiểm thử xử lý batch trên bộ nhỏ. Các trường hợp không có review đã được đối chiếu trực tiếp với API Steam; không có bằng chứng crawler gọi sai bộ lọc hay làm mất review ở 13 game này. Tuy nhiên, danh sách lấy mẫu từ toàn catalog bao gồm game chưa phát hành và game ít hoạt động, nên bộ này không phù hợp để trình diễn đầy đủ trending/realtime hoặc suy rộng về thị trường Steam.

Lượt crawl: 2026-11-12T09:58:51+07:00 → 2026-11-12T10:03:05+07:00 (giờ Việt Nam); 253.9 giây, 127 request. Dừng vì đã đi hết 30 game, không phải hết hạn 25 phút. Báo cáo chỉ kiểm tra raw và áp dụng thử hàm lọc văn bản Python; chưa chạy toàn bộ B1–B8/S1–S3 trên bộ này.

## Khối lượng và chất lượng

| Chỉ số | Kết quả |
| --- | ---: |
| Metadata | 30 bản ghi / 30 game |
| CCU | 19 snapshot / 19 game |
| Review | 2.719 bản ghi / 17 game |
| Game không có review | 13 / 30 |
| Game đạt ít nhất 20 review raw | 10 |
| Review tiếng Anh | 1.857 (68,30%) |
| Ngôn ngữ | 26 |
| Review tích cực | 2.462 (90,55%) |
| Review trùng recommendationid | 0 |
| JSON không đọc được | 0 |
| Review có văn bản rỗng | 4 |
| Thiếu playtime_at_review | 1 |
| Thiếu num_games_owned | 2.719 (API đã bỏ trường này) |
| Bản ghi có source=steam_live | 2.768 / 2.768 |

Các review đều có ID, appid, thời gian tạo/cập nhật, ngôn ngữ, nhãn voted_up và author_steamid. Review ID và SteamID đang là chuỗi. Không có thời gian tạo review vượt thời điểm nhập quá 5 phút. Đây là kiểm tra cấu trúc/phạm vi cơ bản, không chứng minh nội dung từng review là chính xác hoặc không có spam.

Áp dụng trực tiếp cùng hàm clean_review_text/is_spam_or_empty và quy tắc tiếng Anh của B2 dự kiến loại 147 review, giữ 2.572 review; vẫn có 10 game đạt ít nhất 20 review. Đây là dự kiến trước Spark, không phải số liệu đầu ra B2 đã chạy.

## Kiểm tra game không có review

Đã gọi lại từ cursor `*`, filter Recent, tất cả ngôn ngữ, tất cả loại review và purchase_type=All cho cả 13 appid. Cả 13 trả HTTP 200, query_summary.total_reviews=0, total_matching=0, review_score_desc="No user reviews". Game đối chứng Alwa's Legacy trả total_matching=655, khớp 655 review raw đã thu.

- 11 game có ngày phát hành “Coming soon” hoặc trong năm 2027; tất cả 11 cũng nằm trong danh sách failed_sources của CCU.
- RIDLE (1806480) và Water Truck Simulator (4659260) đã phát hành nhưng API vẫn trả 0. Kiểm tra thêm với filter_offtopic_activity=false ở hai game này cũng cho kết quả 0. Không được diễn giải rộng hơn thành không tồn tại bất kỳ review riêng tư/đã xóa nào.
- CCU kiểm tra lại không gửi key ở Chemical Chimpet (964150) và Don't Blink (1585920) trả 404; RIDLE trả HTTP 200, result=1, player_count=0. Đây là kiểm tra mẫu 3 game, không phải xác nhận lại cả 11 lỗi CCU. Thiếu CCU phải giữ null, không tự thay bằng 0.
- Crawler coi trang review trống là kết thúc bình thường. Vì vậy finished và không có lỗi reviews là phù hợp trong các trường hợp API trả dữ liệu trống đã xác minh.

## Độ phủ và độ lệch

- Thời gian review: 15/10/2015 → 12/11/2026 theo giờ Việt Nam; có review trên 1.533 ngày UTC khác nhau trong toàn bộ tập. Không có nghĩa mỗi game có đủ từng ngày hay đủ mật độ theo giờ.
- Trong 7 ngày trước lúc kết thúc crawl: chỉ 7 review / 3 game.
- Trong 14 ngày: 16 review / 7 game; trong 30 ngày: 60 review / 8 game.
- Năm game nhiều review nhất chiếm 89,67% tập. Một game đạt đúng giới hạn 1.200 review nên chưa thể coi đã thu đủ lịch sử của game đó.
- CCU chỉ có một snapshot mỗi game: 15 game có 0 người chơi, hai game có 1, một game có 2, một game có 3. Không có chuỗi lịch sử nhiều ngày.

| Game | Review |
| --- | ---: |
| Ultra Fight Da ! Kyanta 2 (1026840) | 1,200 |
| Alwa's Legacy (1205900) | 655 |
| PIXELMAN (438000) | 278 |
| Cat Isle (4018550) | 209 |
| Living Legends: The Frozen Fear Collection (398620) | 96 |

## Có thể dùng cho những phần nào?

- B2–B5: dùng để kiểm thử làm sạch, tổng hợp, giờ chơi và cảm xúc trên bộ nhỏ; kết quả thống kê chỉ mô tả mẫu này.
- B8: sau áp dụng thử bộ lọc văn bản, còn 10 game có ≥20 review, đủ điều kiện số lượng ban đầu cho k=6. Cần chạy thật để xác minh thêm tính đa dạng đặc trưng và các đầu vào.
- B6: có thể thử phép tính theo ngày, nhưng mật độ review gần đây quá thấp. Xu hướng CCU và tác động giảm giá thiếu lịch sử snapshot; không dùng để kết luận.
- B7/S2: chỉ 7 review trong cửa sổ 7 ngày theo mốc gần cuối tập, nên baseline/cảnh báo có giá trị kiểm thử hạn chế. Review lịch sử dài không đồng nghĩa đủ dữ liệu gần đây.
- S1: có thể thử streaming bằng file hoặc replay Kafka; thời gian sự kiện trải nhiều năm có thể bị watermark loại bỏ, cần tách rõ replay và realtime.
- S3: không có baseline CCU nhiều ngày; mọi snapshot thành công đều dưới ngưỡng baseline tối thiểu mặc định 100, nên không phù hợp minh họa cảnh báo theo cấu hình hiện tại.
- B1/Kafka: có thể kiểm thử chuyển dữ liệu raw qua Kafka bằng replay, nhưng đó là kiểm thử truyền dữ liệu, không phải phát sinh dữ liệu mới từ Steam.

## Các hạn chế crawler cần xử lý trước đợt tiếp theo

1. Chọn game phục vụ demo: kiểm tra đã phát hành, có lượng review và hoạt động gần đây đủ dùng; ghi rõ mẫu lựa chọn có chủ đích thay vì gọi là đại diện toàn thị trường. Vấn đề của lượt này chủ yếu nằm ở tiêu chí chọn mẫu ngẫu nhiên, không phải seed làm dữ liệu giả.
2. Bản ghi review chưa ghi query_summary/total_matching và lý do kết thúc cho từng game. Nên lưu riêng bằng chứng “trống”, “hết trang”, “đạt hạn mức” để lượt sau tự chẩn đoán.
3. API review mặc định lọc off-topic/review bombing; crawler hiện chưa đặt filter_offtopic_activity=false. Muốn đánh giá S2 với review bombing, cần chọn chính sách lọc rõ ràng và ghi vào manifest/state của lượt mới. Điều này không giải thích hai game đã phát hành trả 0 vì đã kiểm tra thêm với false.
4. Metadata hiện lưu giá thiếu thành 0, nên các game chưa bán dễ bị nhầm vào nhóm giá miễn phí khi xử lý. Không nên dùng 11 game chưa phát hành cho phân tích nhóm giá; nên lưu thêm coming_soon và trạng thái giá ở lần thu tiếp.
5. num_games_owned null là giới hạn của API hiện tại, không phải lỗi ghi dữ liệu.

Đề xuất: giữ nguyên bộ này làm bằng chứng thử crawler và batch, tạo lượt demo riêng gồm game đã phát hành, có review gần đây và đủ CCU; bắt đầu tích lũy CCU/metadata định kỳ ngay khi có thể. Không cần xóa bộ hiện tại hoặc gọi lại hàng loạt chỉ để chữa các game chưa phát hành.

## Minh chứng

- `data/raw_trial30/collection-worker-0.json`: báo cáo lượt crawl gốc.
- `data/raw_trial30/collection-audit.json`: thống kê toàn bộ raw và từng game.
- `data/raw_trial30/api-review-check.json`: đối chiếu cả 13 game trống và một game đối chứng.
- `data/raw_trial30/api-extra-check.json`: kiểm tra off-topic ở hai game đã phát hành và một game chưa phát hành; CCU ở ba game.
- [Tài liệu API review Steam](https://partner.steamgames.com/doc/webapi/IUserReviewsService): enum, cursor, tổng review và bộ lọc off-topic.

## Chi tiết từng game

| AppID | Game | Review | Ngày UTC có review | Review tiếng Anh | CCU | Phát hành (metadata) |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 398620 | Living Legends: The Frozen Fear Collection | 96 | 80 | 42 | 0 | Oct 13, 2015 |
| 411090 | Witness the Dark - Episode 1 - Bloody Burger | 34 | 22 | 14 | 1 | Aug 9, 2026 |
| 438000 | PIXELMAN | 278 | 251 | 98 | 0 | Feb 28, 2018 |
| 453660 | Rogue Contracts: Syndicate | 32 | 32 | 22 | 0 | Aug 26, 2016 |
| 853300 | The Pizza Delivery Boy Who Saved the World | 32 | 32 | 30 | 0 | Sep 6, 2018 |
| 898170 | Grid Gunner | 2 | 2 | 2 | 0 | Jul 27, 2018 |
| 964150 | Chemical Chimpet | 0 | 0 | 0 | thiếu | Coming soon |
| 1026840 | Ultra Fight Da ! Kyanta 2 | 1200 | 841 | 1018 | 0 | May 8, 2019 |
| 1205900 | Alwa's Legacy | 655 | 527 | 406 | 1 | Jun 17, 2020 |
| 1585920 | Don't Blink | 0 | 0 | 0 | thiếu | Coming soon |
| 1714530 | Blood of Mehran | 77 | 32 | 48 | 0 | Oct 6, 2025 |
| 1744960 | 墨卷奇路 | 1 | 1 | 0 | 0 | Oct 28, 2026 |
| 1806480 | RIDLE | 0 | 0 | 0 | 0 | Aug 17, 2025 |
| 1883480 | Hockey Shooter VR | 5 | 5 | 5 | 0 | Feb 18, 2022 |
| 2099570 | City Massacre | 0 | 0 | 0 | thiếu | Coming soon |
| 3069460 | Spyeye | 0 | 0 | 0 | thiếu | Coming soon |
| 3635540 | Spectral Protocol | 0 | 0 | 0 | thiếu | Coming soon |
| 3910190 | Stout Kayak | 9 | 8 | 5 | 0 | Oct 9, 2025 |
| 3913090 | Dawn After Dusk | 6 | 6 | 5 | 0 | Sep 9, 2025 |
| 4018550 | Cat Isle | 209 | 64 | 151 | 3 | Jul 20, 2026 |
| 4226820 | GRIDBUSTERS | 2 | 2 | 2 | 0 | Apr 29, 2026 |
| 4302590 | Particle Collectotron | 0 | 0 | 0 | thiếu | Q1 2027 |
| 4583960 | Sister Compass: Slow Life Loving with My Sisters on a Deserted Island | 76 | 18 | 8 | 2 | Oct 7, 2026 |
| 4659260 | Water Truck Simulator | 0 | 0 | 0 | 0 | Jun 19, 2026 |
| 4865860 | Manual Override | 5 | 3 | 1 | 0 | Sep 27, 2026 |
| 5036760 | Project Turbo Combat | 0 | 0 | 0 | thiếu | Q2 2027 |
| 5140780 | Protos. Neural Collapse | 0 | 0 | 0 | thiếu | Coming soon |
| 5313430 | Give him a break! | 0 | 0 | 0 | thiếu | Coming soon |
| 5350290 | Zanore: Echoes of the Shards | 0 | 0 | 0 | thiếu | January 2027 |
| 5358080 | Sunder the Trident | 0 | 0 | 0 | thiếu | Coming soon |
