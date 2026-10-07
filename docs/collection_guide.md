# Thu thập dữ liệu Steam vào JSONL nén

Chạy từ thư mục gốc dự án, bằng Python trong `venv`. Chế độ này ghi file trên máy, không cần Kafka/HDFS. Sử dụng danh sách 1.000 game đã có và giới hạn 3.000 request Steam cho một lượt; dữ liệu lịch sử được nhập riêng để tăng số bản ghi.

## 1. Danh sách game

`data/target_apps.json` đã chứa 1.000 game; crawler và backfill đọc trực tiếp file này. Mỗi mục có `appid` cùng thông tin tham khảo về game. `data/target_apps.json.manifest.json` lưu nguồn catalog, revision, SHA-256 và phương pháp chọn danh sách hiện tại.

Để tạo lại hoặc mở rộng danh sách, dùng catalog công khai [FronkonGames/steam-games-dataset](https://huggingface.co/datasets/FronkonGames/steam-games-dataset). Lệnh tải Parquet vào `data/cache/catalog`, xếp hạng và ghi target cùng manifest. Điểm chọn kết hợp thứ hạng tổng review (70%) với thứ hạng `peak_ccu` trong catalog (30%): `0.7/(60+review_rank) + 0.3/(60+ccu_rank)`, đồng thời loại các thể loại phần mềm. CCU đang online vẫn được lấy riêng qua API khi crawl.

```powershell
# Tạo danh sách 2.000 game vào file riêng để giữ danh sách hiện tại.
.\venv\Scripts\python.exe -B -m crawlers.build_targets --limit 2000 --output data/target_apps_2000.json

# Crawl danh sách mới: 3 nguồn/game, tối đa 6.000 request.
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --targets data/target_apps_2000.json --limit 2000 --max-requests 6000
```

Nếu chỉnh danh sách thủ công, dùng appid hợp lệ và không vượt quá 2.000 game. Khi đổi tập game, dùng thư mục checkpoint lịch sử mới; chương trình từ chối tiếp tục checkpoint có tập game khác. Giữ nguyên danh sách và thứ tự nếu tiếp tục một lượt Steam đang dang dở. `--limit` trên crawler chỉ giới hạn số game đọc từ file, không bổ sung game mới.

## 2. Crawl dữ liệu hiện tại

```powershell
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --limit 1000 --max-requests 3000 --interval 1.5 --review-batches 1
```

Mỗi game được gọi lần lượt Store appdetails, CCU và Reviews. Một batch review lấy tối đa 100 review, tất cả ngôn ngữ. Mặc định một worker thực hiện toàn bộ danh sách; CLI này không dùng cấu hình 4 worker trong `.env`.

3.000 request cách nhau tối thiểu 1,5 giây có thời gian pacing khoảng 75 phút; độ trễ mạng và cooldown có thể làm lâu hơn. Retry review cũng được tính vào ngân sách. Chương trình dừng khi hết ngân sách hoặc có 5 lần lỗi liên tiếp. SteamSpy là nguồn tùy chọn, thêm qua `--sources steamspy`; không nằm trong đợt mặc định.

Xem tiến độ của đợt đang chạy nền:

```powershell
Get-Content .\data\raw\collection-worker-0.json
Get-Content .\data\logs\live-collection.stderr.log -Tail 20 -Wait
```

`Ctrl+C` ở lệnh xem log chỉ dừng theo dõi log. Thông tin tiến trình nền nằm trong `data/raw/collection-process.json`. Để dừng tiến trình đã được khởi chạy:

```powershell
$collectionJob = Get-Content .\data\raw\collection-process.json | ConvertFrom-Json
Stop-Process -Id $collectionJob.pid
```

Trạng thái `finished` nghĩa là đã thử toàn bộ game được giao; xem `failures` và `failed_sources` để biết game/nguồn nào cần gọi lại. `completed_games` là số game đã đi qua các nguồn trong lượt này; `records` và `requests` cũng chỉ tính lượt hiện tại.

Nếu bị ngắt hoặc hết request, lấy `next_game_index` để tiếp tục từ game chưa hoàn thành, với **cùng danh sách, thứ tự, sources và worker**. Đảm bảo tiến trình cũ đã dừng trước khi chạy tiếp:

```powershell
$collectionStatus = Get-Content .\data\raw\collection-worker-0.json | ConvertFrom-Json
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --limit 1000 --max-requests 3000 --start-index $collectionStatus.next_game_index
```

Game bị ngắt giữa chừng sẽ được gọi lại metadata/CCU, tạo snapshot mới. Review tiếp tục từ cursor đã ghi sau khi dữ liệu được flush. Một lần chạy không có `--start-index` bắt đầu lượt mới từ game đầu tiên; riêng cursor review vẫn tiếp tục về các review cũ hơn. Muốn chỉ lấy review mới nhất, dùng thư mục `--state-dir` riêng cho lượt đó và khử trùng review sau khi nhập.

### Chạy lại riêng nguồn bị lỗi

Đọc `failed_sources` trong báo cáo và tìm appid tương ứng trong log. Timeout có thể thử lại sau vài giây; giới hạn 2–3 lần thử. Chỉ gọi nguồn/appid bị lỗi, giữ nguyên state review để tiếp tục đúng cursor. Ví dụ tạo target riêng rồi chạy:

```powershell
'[261570]' | Set-Content .\data\cache\retry-reviews.json -Encoding UTF8
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --targets data/cache/retry-reviews.json --sources reviews --max-requests 3 --output-dir data/raw/retry-reviews

'[447530]' | Set-Content .\data\cache\retry-ccu.json -Encoding UTF8
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --targets data/cache/retry-ccu.json --sources ccu --max-requests 3 --output-dir data/raw/retry-ccu
```

Dùng output riêng để giữ báo cáo đợt đầu. Những lệnh trên vẫn dùng `data/crawler_state` mặc định. Timeout không tự được thử lại trong cùng lượt CLI; nếu lượt nhỏ vẫn lỗi thì đợi rồi gọi lại, tối đa số lần bạn chọn. Khi xử lý, đọc thêm các thư mục topic bên trong `data/raw/retry-*`.

Hai lỗi timeout của đợt đầu (Reviews `261570`, CCU `447530`) đã được gọi lại thành công lúc 15:39 ngày 07/10/2026, bổ sung 100 review và 1 snapshot CCU vào các thư mục topic ban đầu. Báo cáo xác nhận nằm ở `data/raw/collection-retry.json`; báo cáo đợt đầu vẫn giữ nguyên lịch sử 2 lỗi. Không cần chạy lại hai ví dụ này cho đợt đã khôi phục.

CCU lấy lại là số người đang chơi tại thời điểm gọi lại; API không khôi phục được snapshot tại thời điểm timeout trước đó.

## 3. Nhập review lịch sử công khai

```powershell
.\venv\Scripts\python.exe -u -B -m crawlers.historical_backfill --limit 1000000 --max-download-mb 1024
```

Nguồn là [GianLucaSpagnolo/Steam_Reviews_Dataset_2021](https://huggingface.co/datasets/GianLucaSpagnolo/Steam_Reviews_Dataset_2021), được tải theo các shard Parquet của revision được ghi trong manifest. Chỉ review của các app trong danh sách chọn mới được xuất. Giữ nguyên `recommendationid`, thời gian tạo/cập nhật review và thông tin nguồn; `ingest_timestamp` ghi thời điểm nhập trên máy.

Dataset nguồn có khoảng 21,7 triệu review thuộc 202 game; số game giao với danh sách 1.000 sẽ ít hơn hoặc bằng 202. Các shard có thứ tự cố định, nên lượt giới hạn đầu tiên có thể tập trung vào vài game. Dữ liệu lịch sử này tăng số bản ghi và chiều thời gian, không đảm bảo phân bố đều trên 1.000 game. Kiểm tra độ phủ và lấy mẫu theo game khi phân tích.

`--limit` là số review **bổ sung trong lần chạy**, không phải tổng số đã nhập. Chạy lại cùng lệnh/tập game/thư mục state để tiếp tục. Các shard hoàn thành được bỏ qua; shard dang dở tiếp tục tại hàng đã checkpoint. `--max-download-mb` giới hạn tổng kích thước shard chưa hoàn thành được xử lý trong lượt, kể cả shard đã có trong cache. Cache được giữ để tái sử dụng và có thể tăng trên 1 GB sau nhiều lượt; raw và catalog cũng dùng dung lượng riêng.

Tiến độ nằm ở `data/raw/historical-backfill.json`; checkpoint từng shard nằm trong `data/crawler_state/history/<revision>/`. Thay đổi file nguồn/tập game bị từ chối để tránh bỏ sót dữ liệu khi resume.

Đợt đầu đã ghi nhận 1.000.000 review lịch sử trong checkpoint sau khi chạy tiếp 772.000 review từ 228.000 review đã lưu. Raw có thể lặp sau sự cố ghi checkpoint; báo cáo kiểm tra nằm trong `data/raw/collection-audit.json`.

## 4. File đầu ra và xử lý tiếp

```text
data/raw/steam_games_meta/dt=YYYY-MM-DD/part-*.jsonl.gz
data/raw/steam_players_ccu/dt=YYYY-MM-DD/part-*.jsonl.gz
data/raw/steam_reviews_raw/dt=YYYY-MM-DD/part-*.jsonl.gz
```

Mỗi dòng là một object JSON UTF-8; thư mục `dt` là ngày nhập UTC, không phải ngày tạo review. Phân tích review theo `timestamp_created`, snapshot CCU theo `timestamp`. File `.tmp` chưa hoàn tất không được tính là đầu ra. Output được flush trước checkpoint; sau sự cố có thể phát lại một phần đã ghi (at least once).

Khi đưa vào Spark/Parquet, khử trùng review bằng `recommendationid`, ưu tiên `timestamp_updated` lớn nhất rồi `ingest_timestamp` lớn nhất. Giữ SteamID/review ID dưới dạng chuỗi để tránh mất độ chính xác. Đếm review lịch sử và review live theo `source`; không cộng trùng ID giữa hai nguồn. Snapshot metadata/CCU có thời điểm riêng, nên không khử trùng chỉ bằng appid.

Raw, cache và checkpoint được gitignore. Lưu bản sao các thư mục này cùng target list và manifest nếu cần chuyển máy; không xóa state trước khi tiếp tục lượt đang dở.

## 5. Lịch thu tiếp để kiểm soát chi phí

Sau lượt đầu, tách nguồn theo tần suất: metadata mỗi tuần; CCU khoảng 30–60 phút/lượt; review hàng ngày. Một lượt CCU 1.000 game có tối thiểu khoảng 25 phút pacing với interval 1,5 giây. Chạy các lượt tuần tự để giới hạn tốc độ tổng, tránh chồng nhiều tiến trình cùng checkpoint.

```powershell
# Một lượt CCU: có thể lặp theo lịch bạn chọn sau khi lượt trước kết thúc.
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --sources ccu --limit 1000 --max-requests 1000

# Review: cùng state lấy thêm các trang cũ, state riêng lấy từ trang mới nhất.
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --sources reviews --limit 1000 --max-requests 1100 --review-batches 1

# Metadata cập nhật định kỳ.
.\venv\Scripts\python.exe -u -B -m crawlers.collect_local --sources metadata --limit 1000 --max-requests 1000
```

CCU API chỉ trả số người đang chơi tại thời điểm gọi. Lịch sử CCU được tích lũy từ các lượt crawl tiếp theo; dataset review 2021 không cung cấp chuỗi CCU lịch sử.

Kiểm thử mã thu thập:

```powershell
.\venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
```
