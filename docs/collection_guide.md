# Thu thập dữ liệu trực tiếp từ Steam

Quyết định ngày 12/11/2026: chỉ dùng API Steam. Không nhập Hugging Face, Kaggle hoặc SteamSpy, kể cả cho danh sách game. Các lệnh dưới là Bash, chạy tại gốc repo.

## 1. Môi trường và cấu hình

```bash
conda activate steam-insights
# Nếu tạo môi trường mới:
# python -m pip install -r requirements-crawler.txt
```

Giữ `STEAM_API_KEY` thật trong `.env`, không commit hoặc đưa vào log. Crawler nạp `.env` tại gốc repo; biến môi trường đã export có ưu tiên cao hơn file này.

```dotenv
STEAM_API_KEY=<key của bạn>
CRAWLER_WORKER_ID=0
CRAWLER_TOTAL_WORKERS=1
CRAWLER_RATE_LIMIT_DELAY=1.5
TARGET_APP_LIST_PATH=data/target_apps.json
CURSOR_STATE_DIR=data/crawler_state
CRAWLER_OUTPUT=jsonl
CRAWLER_OUTPUT_DIR=data/raw
CRAWLER_TARGET_LIMIT=1000
CRAWLER_REVIEW_BATCHES=10   # trang review (100 review/trang) tối đa mỗi game mỗi lượt
CRAWLER_REVIEW_DAYS=0       # backfill review tới N ngày trước; 0 = toàn bộ lịch sử (mặc định trong code: 90)
```

`collect_local` luôn ghi JSONL local. Các crawler chạy riêng dùng `CRAWLER_OUTPUT=jsonl|kafka`. Tuỳ chọn CLI ghi đè mặc định từ `.env`. Chạy một worker trên máy cá nhân; nhiều tiến trình sẽ cộng dồn tốc độ request và không được dùng chung state.

Các endpoint (đều của Steam):

| Nguồn (`--sources`) | Endpoint | Dữ liệu | Request |
|---|---|---|---|
| chọn game | `IStoreService/GetAppList/v1` (cần key) | catalog game | ~5 / lần build |
| chọn game | `ISteamChartsService/GetMostPlayedGames/v1` | top ~100 theo CCU, hạng tuần trước | 1 |
| `store`, chọn game | `IStoreBrowseService/GetItems/v1` | tổng review, % tích cực, ngày phát hành, coming soon, early access, **tag + trọng số**, Steam Deck | 1 / 200 game |
| `store` | `IStoreService/GetTagList/v1` | tên tag | 1 |
| `metadata` | `store.steampowered.com/api/appdetails` | giá, giảm giá, thể loại, category, nhà phát triển/phát hành, nền tảng, Metacritic, DLC, achievement, số ngôn ngữ, độ tuổi | 1 / game |
| `histogram` | `store.steampowered.com/appreviewhistogram/{appid}` | số review up/down **theo ngày** 30 ngày gần nhất (mọi ngôn ngữ) + theo tháng cả vòng đời | 1 / game |
| `ccu` | `ISteamUserStats/GetNumberOfCurrentPlayers/v1` | số người đang chơi lúc gọi | 1 / game |
| `reviews` | `IUserReviewsService/GetAppReviews/v1` | nội dung, voted_up, thời gian, giờ chơi (tổng / lúc review / 2 tuần / Deck), last_played, vote hữu ích/vui, comment, mua trên Steam, nhận miễn phí, refund, early access, Steam Deck | ≤ `CRAWLER_REVIEW_BATCHES` / game |

Không dùng SteamSpy: tag lấy từ `GetItems` (chính thức), giờ chơi lấy từ từng review, quy mô game dùng tổng review thay cho số người sở hữu ước lượng.

`STEAM_WEB_API_BASE` và `STEAM_STORE_API_BASE` có mặc định là máy chủ Steam; chỉ ghi đè khi kiểm thử mock.

## 2. Tạo và mở rộng danh sách game

```bash
python -B -m crawlers.build_targets --limit 100 --top-ccu 20 --seed 42
```

Gồm hai nhóm, ghi trong trường `group` của từng game:

- `top_ccu`: `--top-ccu` game đứng đầu bảng most-played của Steam (đã phát hành, có ≥ `--min-reviews`). Nhóm này có hoạt động liên tục, dùng cho streaming/realtime và CCU.
- `stratified`: phần còn lại. Catalog được xáo bằng `--seed`, thăm dò theo lô 200 qua `GetItems`, **bỏ game chưa phát hành và game dưới `--min-reviews` (mặc định 20)**, rồi chia đều chỉ tiêu cho 4 tầng số review `20-99`, `100-999`, `1k-9.9k`, `10k+`. Tầng nào thiếu thì bù từ tầng khác và ghi `topped_up` trong manifest.

**Mở rộng sau này không trùng:** chạy lại cùng `--output` với `--limit` lớn hơn. Game cũ giữ nguyên vị trí đầu danh sách, game mới loại trừ mọi appid đã có, manifest thêm một mục trong `batches` (seed, ngưỡng, số đã thăm dò, chỉ tiêu từng tầng). State review của game cũ được dùng tiếp, nên dữ liệu đã thu không phải lấy lại.

```bash
python -B -m crawlers.build_targets --limit 1000 --top-ccu 50 --seed 43
```

**Vòng phản hồi (hằng ngày):** thêm tối đa `--top` game đang lên trên bảng most-played (mới vào top hoặc tăng hạng so với tuần trước), nhóm `feedback`, chỉ thêm không xoá, ghi một mục `kind: feedback` trong manifest:

```bash
python -B -m crawlers.update_targets_from_trending --top 5    # hoặc: python pipeline.py feedback -- --top 5
```

`--fresh` bỏ qua danh sách cũ (khi đó dùng raw và state riêng). Đây **không phải mẫu đại diện của toàn catalog**: game dưới ngưỡng review và game chưa phát hành bị loại có chủ đích.

## 3. Crawl

```bash
python -u -B -m crawlers.collect_local --max-requests 2000
```

Thứ tự: `store` cho toàn danh sách, rồi từng game `metadata → histogram → ccu → reviews`. 100 game với 10 trang review tối đa khoảng 1.300 request, khoảng 35 phút ở interval 1,5 giây.

Review **tăng dần, không trùng** (state ở `CURSOR_STATE_DIR/reviews_v2/reviews_<appid>.json`). Mỗi lượt, trong hạn mức trang của game:

1. Lấy review mới hơn review mới nhất của lượt trước, dừng ngay khi gặp review đã có.
2. Nếu lượt trước hết hạn mức giữa chừng phần review mới, lấy nốt phần đó (không để lỗ hổng).
3. Dùng phần trang còn lại để backfill review cũ tới `--review-days`.

Một review chỉ được ghi một lần qua mọi lượt, kể cả khi lỗi mạng, hết ngân sách hay `Ctrl+C` (đã kiểm thử ngẫu nhiên 13.000 kịch bản). Muốn thêm lịch sử chỉ cần chạy với `--review-days` lớn hơn hoặc `0`; backfill tiếp từ chỗ đã dừng. Ngoại lệ duy nhất là tiến trình bị kill cứng giữa lúc ghi một trang và lúc lưu state: trang đó có thể bị ghi lại, B2 vẫn khử trùng theo `recommendationid`.

Game lớn nhận hàng nghìn review mỗi ngày nên 10 trang chỉ phủ vài giờ đến vài ngày gần nhất. Số lượng review theo ngày để tính xu hướng lấy từ `histogram`, không đếm từ review đã crawl.

## 4. Tiến độ, tiếp tục và chạy lại lỗi

```bash
cat data/raw/collection-worker-0.json
```

`finished` chỉ có nghĩa đã thử hết danh sách. Xem `failures` và `failed_sources`; `records`, `requests`, `completed_games` chỉ tính lượt hiện tại. `Ctrl+C` dừng lượt đó, lưu state review và ghi báo cáo cuối.

Tiếp tục một lượt bị ngắt:

```bash
NEXT=$(python -c 'import json; print(json.load(open("data/raw/collection-worker-0.json"))["next_game_index"])')
python -u -B -m crawlers.collect_local --max-requests 2000 --start-index "$NEXT"
```

Chạy lại không có `--start-index` cũng an toàn với review (không trùng); metadata/histogram/CCU chỉ thêm một snapshot.

## 5. Đầu ra và xử lý

```text
data/raw/steam_games_meta/dt=YYYY-MM-DD/part-*.jsonl.gz    # source = steam_live | steam_store_browse | steam_review_histogram
data/raw/steam_players_ccu/dt=YYYY-MM-DD/part-*.jsonl.gz
data/raw/steam_reviews_raw/dt=YYYY-MM-DD/part-*.jsonl.gz
```

`dt` là ngày nhập UTC; review phân tích theo `timestamp_created`, CCU/metadata theo `timestamp`. B2 tạo:

- `clean/reviews`: khử trùng theo `recommendationid`.
- `clean/games`: appdetails mới nhất + snapshot store mới nhất (tag, tổng review, early access).
- `clean/games_history`: mọi snapshot appdetails (lịch sử giá/giảm giá).
- `clean/review_daily_counts`: `(appid, dt, reviews_up, reviews_down, reviews_total)`, giữ snapshot mới nhất cho mỗi ngày.
- `clean/ccu`.

Metadata, histogram, CCU là snapshot theo thời gian: chạy nhiều lần là có thêm điểm dữ liệu, không phải bản ghi trùng.

```bash
python pipeline.py --profile local batch --raw data/raw --lake data/lake
```

## 6. Thu định kỳ

Dùng **cùng** `--state-dir` cho mọi lượt. Chạy tuần tự, tránh chồng tiến trình. Vòng lặp chạy sẵn cả ba lịch dưới đây nằm ở README mục 4.3 (A6). Vòng lặp chỉ chạy khi máy bật: máy tắt hoặc ngủ thì CCU của khoảng đó bị mất, không lấy lại được, nên cần một máy bật 24/7, VM hoặc CronJob trên cụm.

```bash
# CCU: 10–15 phút/lượt, liên tục ≥ 14 ngày để có xu hướng CCU (Steam không có API lịch sử CCU).
python -u -B -m crawlers.collect_local --sources ccu --max-requests 300
# Review mới: 30–60 phút/lượt.
python -u -B -m crawlers.collect_local --sources reviews --max-requests 1500
# Store, metadata (giá), histogram: hằng ngày.
python -u -B -m crawlers.collect_local --sources store metadata histogram --max-requests 500
```

**Đưa crawler vào Kafka (demo streaming):** đặt `CRAWLER_OUTPUT=kafka` ngay trong lệnh và dùng state riêng (`CURSOR_STATE_DIR=data/crawler_state/stream`), để review đã gửi Kafka không làm lệch tiến độ crawl vào `data/raw`. Xem README mục 4.11.

```bash
python -m pytest -q tests/unit/test_steam_collection.py tests/unit/test_review_incremental.py tests/unit/test_text_and_targets.py tests/unit/test_feedback.py
```
