# Các quyết định xây dựng và triển khai

Ghi lại các lựa chọn nhóm đã chốt, kèm lý do, để thành viên mới không phải hỏi lại. Khi đổi một quyết định, sửa mục tương ứng và ghi ngày đổi. Bản thiết kế gốc: `BigData.pdf`. Phân tích tài nguyên: [van_de_va_giai_phap_ha_tang.md](van_de_va_giai_phap_ha_tang.md). Tiến độ: [BAO_CAO_TIEN_DO.md](BAO_CAO_TIEN_DO.md), [PLAN.md](PLAN.md).

Cập nhật lần cuối: 2026-11-12.

## 1. Môi trường làm việc

### 1.1. Phát triển và kiểm chứng logic trên máy cá nhân

**Chốt:** mọi việc viết và sửa job, làm dashboard, gom dữ liệu đều làm trên máy cá nhân, không cần Kubernetes.

- Spark chạy local mode qua `python pipeline.py ...` (profile `local`: raw = `data/raw`, đầu ra = `data/lake/`). Không cần Kafka/HDFS/ES để chạy batch layer.
- Khi cần dịch vụ, dùng `scripts/demo.sh` với `compose.demo.yaml` và **bật từng nhóm một**: `batch` (HDFS), `streaming` (Kafka), `dashboard` (ES + Kibana). Mỗi container giới hạn 1 GB RAM.
- Thử logic trên dữ liệu mẫu trước: `python tests/make_sample_data.py` (có sẵn review bombing, CCU spike, đợt giảm giá).

**Lý do:** đã kiểm chứng ngày 2026-10-26 trên WSL 7,6 GB RAM (B2→B8 và S2 trên dữ liệu mẫu), và ngày 2026-11-12 trên WSL 8 GB với dữ liệu thật 100 game: batch B2→B8 ghi ES, Kafka → S1–S3 → ES, Kibana D1–D4 (README mục 4.11). Chỉ bật toàn bộ hệ thống trên K8s mới cần nhiều RAM.

| Việc | Cần bật | RAM ước tính |
| --- | --- | --- |
| Batch layer trên dữ liệu thật | Spark local | 2–3 GB |
| Dashboard Kibana (`pipeline.py batch --es`) | ES + Kibana | 4–5 GB (đo 12/11: ES khoảng 0,8 GB, Kibana khoảng 0,5 GB, cộng Spark) |
| Streaming với Kafka (`s1`–`s3`) | Kafka 1 broker + Spark | 3–4 GB (đo 12/11: Kafka khoảng 0,3 GB, mỗi job khoảng 1 GB) |
| Crawl, backfill | Python | < 0,5 GB |

### 1.2. Cụm phân tán trên Google Cloud, chạy theo buổi

**Chốt:** cụm Kubernetes nhiều node dùng cho tích hợp, kiểm thử chịu lỗi (FT) và mở rộng (SC) chạy trên **Google Cloud Compute Engine**, cài **k3s** và nối các VM bằng **Tailscale**. Không dùng GKE.

- Cấu hình: **4 VM `e2-standard-2`** (2 vCPU, 8 GB mỗi VM), tổng 8 vCPU / 32 GB, Ubuntu 24.04 LTS. Một VM làm k3s server, ba VM làm agent.
- Cài đặt theo `infra/k3s/README.md` (dựng cụm, demo) và `infra/k3s/FAULT_TEST.md` (kiểm thử).
- Nguồn tiền: GCP Free Trial (300 USD trong 90 ngày cho tài khoản mới; trial giới hạn khoảng 8 vCPU chạy cùng lúc, kiểm tra lại điều kiện trên trang GCP trước khi tạo).
- **Chỉ bật VM trong buổi làm việc, tắt (stop) ngay khi xong.** Ước tính khoảng 0,25–0,3 USD/giờ cho cả cụm cộng tiền ổ đĩa; bật 24/7 sẽ tốn khoảng 200 USD/tháng.
- Đặt cảnh báo ngân sách (Budget alert) trên GCP ngay khi tạo project.

**Lý do không dùng GKE:** manifest trong `k8s/demo` và `k8s/fault-test` dùng StorageClass `local-path`, nạp image trực tiếp vào node (`imagePullPolicy: Never`) và nhãn node riêng. Chuyển sang GKE phải dùng Artifact Registry, đổi StorageClass và nhãn, tức là làm lại phần hạ tầng.

**Lý do giữ Tailscale trên VM cloud:** `infra/k3s/install-node.sh` bắt buộc có interface `tailscale0` (node IP và Flannel đi qua Tailscale). Giữ Tailscale thì không phải sửa script, và có thể ghép thêm máy của thành viên vào cùng cụm khi cần.

**Phương án dự phòng** (theo thứ tự): Azure for Students (100 USD, không cần thẻ, cần email sinh viên); ghép máy cá nhân các thành viên qua Tailscale; Oracle Cloud Always Free (4 CPU ARM / 24 GB, thường hết tài nguyên, phải build image ARM64).

## 2. Cấu hình thu gọn so với thiết kế gốc

Theo [van_de_va_giai_phap_ha_tang.md](van_de_va_giai_phap_ha_tang.md). Đây là cấu hình đang dùng trong `k8s/fault-test`; báo cáo phải ghi rõ các điểm khác thiết kế.

| Thành phần | Thiết kế gốc | Đang dùng | Ghi chú |
| --- | --- | --- | --- |
| Kafka | 3 broker, RF=3, min ISR=2 | Giữ nguyên (KRaft) | Cần cho FT1 có ý nghĩa |
| HDFS | 1 NameNode + 3 DataNode, replication 3 | 1 NameNode + 2 DataNode, replication 2 | Tiết kiệm RAM/đĩa |
| Elasticsearch | 3 node, 1 replica | 1 node, 0 replica | Chưa kiểm thử FT4/SC4 |
| Hive Metastore | Có | Chưa triển khai | Truy vấn ad-hoc bằng Spark SQL trên Parquet (`createOrReplaceTempView`); job tự đăng ký bảng nếu sau này có Metastore |
| Prometheus/Grafana | Có | Chưa triển khai | Tạm dùng `kubectl top` và Kafka consumer lag |
| Quy mô | 1.000–2.000 game, 20–30 GB | Demo: 100 game (từ 12/11). Sau buổi báo cáo tiến độ mở rộng khoảng 1.000 game (tối đa 2.000) | Danh sách chỉ mở rộng, crawl tăng dần; cần ≥ 14 ngày dữ liệu cho B6/B7 |

## 3. Quy ước code và vận hành

- **Thứ tự batch:** B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8, rồi `merge_views` khi ghi ES. B3 dùng cảm xúc từ B5; B7 tạo baseline cho S2/S3. `Makefile` và `pipeline.py` dùng cùng thứ tự này.
- **Layout dữ liệu:** raw `…/raw/{steam_reviews_raw|steam_players_ccu|steam_games_meta}/dt=YYYY-MM-DD/*.jsonl.gz`. Crawler local và B1 ghi cùng layout, nên dữ liệu crawl dùng trực tiếp làm raw.
- **Tham số job:** `--raw`, `--clean`, `--curated` là **thư mục gốc**, không trỏ tới thư mục con của từng nguồn.
- **Chạy lại an toàn:** B1 lưu offset; B2–B8 tính lại từ dữ liệu nguồn và ghi đè. Không cần đổi thư mục đầu ra khi chạy lại.
- **Ghi Elasticsearch:** dùng Python client (`processing/common/es_writer.py`), không dùng connector JVM. Bật bằng `WRITE_ES=1` hoặc `--es-nodes`. Mapping chuẩn ở `storage/elasticsearch/indices_mapping.json`; mapping demo (`infra/demo/elasticsearch-indices.json`) sinh từ mapping chuẩn với 1 shard / 0 replica.
- **Image Spark cho k3s:** image chứa bản sao code tại thời điểm build. **Mỗi khi `processing/` hoặc `crawlers/` thay đổi phải build lại và tăng tag** (dạng `k3s-YYYYMMDD`), sửa tag ở mọi manifest. Tag hiện tại: `localhost/steam-spark-demo:k3s-20261026` — **cũ hơn code ngày 12/11, phải build lại trước khi lên cụm**.
- **Thời gian:** mọi mốc lưu trữ và ghi ES theo UTC. Baseline CCU theo khung giờ UTC.
- **Sự kiện demo:** chỉ bơm qua `pipeline.py inject` (`source=demo_injection`). B2 loại các bản ghi này. Không dùng `replay --retime` để demo realtime, vì nó dồn review cũ vào hiện tại và làm game nhỏ trông như bị bombing; chỉ dùng để thử tải (SC3).
- **Phân tích văn bản:** chỉ phân tích nội dung review tiếng Anh (VADER, từ điển khía cạnh, TF-IDF); ngôn ngữ khác chỉ dùng nhãn `voted_up`.

## 4. Thứ tự công việc đã thống nhất

1. **Máy cá nhân:** chạy pipeline trên dữ liệu thật và sửa lỗi; dựng dashboard Kibana D1–D4 (xong 12/11); cho vòng thu chạy liên tục trên một máy bật 24/7 (đang làm).
2. **Trước khi bật cloud:** viết manifest Job/Deployment trên k3s cho B1–B8 và S1–S3 (hiện mới có B2 và smoke), để thời gian trên cloud chỉ dùng để chạy.
3. **Trên cloud, theo buổi:** dựng cụm 4 VM, chạy trọn pipeline, chạy FT/SC theo `infra/k3s/FAULT_TEST.md`, lưu kết quả vào `tests/results/`, chụp màn hình và quay video. Tắt VM sau mỗi buổi.
4. **Bàn giao:** notebook, `docs/architecture.md`, trả lời 5 câu hỏi §1.3 bằng số liệu, báo cáo, slide, video.

## 5. Chưa chốt, cần hỏi giảng viên

Theo mục 6 của [van_de_va_giai_phap_ha_tang.md](van_de_va_giai_phap_ha_tang.md):

1. Bỏ Hive Metastore, dùng Spark SQL đọc trực tiếp Parquet có được chấp nhận không?
2. Cụm k3s trên VM cloud và máy cá nhân nối qua VPN có được xem là "mô phỏng môi trường phân tán" không?
3. "Không dùng Docker" có cấm cả container runtime trong K8s (containerd của k3s) và Compose dùng để thử local không?
4. Bộ demo 100 game và mục tiêu khoảng 1.000 game, chỉ lấy từ API Steam (không dùng dataset ngoài), có đủ không?
5. Giảm HDFS replication 3 → 2 và Elasticsearch 3 → 1 node có ảnh hưởng điểm phần kiểm thử chịu lỗi không?

## 6. Nguồn thu thập dữ liệu (2026-11-12)

**Chốt:** chỉ thu thập trực tiếp qua API Steam, dùng cấu hình `.env`. Không dùng Hugging Face, Kaggle hay SteamSpy, kể cả để chọn danh sách game. Review lịch sử lấy bằng phân trang API Steam. Mỗi game có cùng hạn mức số trang trong một lượt; không bù số lượng bằng dataset ngoài.

**Cập nhật cách chọn game:** lấy mẫu đều trên toàn catalog cho ~70% game có dưới 20 review và ~29% game chưa phát hành (lần thử 30 game: 13 game không có review, 11 game lỗi CCU). Danh sách mới gồm nhóm `top_ccu` (bảng most-played chính thức của Steam) và nhóm `stratified` (mẫu có seed, đã lọc game đã phát hành và ≥ 20 review, chia đều 4 tầng số review). Ghi rõ trong báo cáo: nhóm `top_ccu` là game phổ biến, nhóm `stratified` không đại diện cho game dưới ngưỡng. Danh sách chỉ được mở rộng, không xáo lại; review crawl tăng dần nên lần thu lớn sau dùng lại toàn bộ dữ liệu đã có. Tag lấy từ `IStoreBrowseService/GetItems`, số review theo ngày từ `appreviewhistogram` thay cho SteamSpy. Raw/lake cũ trộn nguồn không dùng cho kết quả mới; nếu máy nào còn, chuyển vào `data/legacy_nonsteam/`.

## 7. Giai đoạn dữ liệu (2026-11-12)

**Chốt:**
1. **Báo cáo tiến độ:** demo với 100 game cố định; vòng phản hồi tắt.
2. **Sau báo cáo:** thu liên tục và mở rộng danh sách (khoảng 1.000 game).
3. **Gần ngày bảo vệ:** bật vòng phản hồi, mỗi ngày thêm tối đa 5 game đang lên trên bảng most-played.

Chỉ một nơi crawl tại một thời điểm; chuyển nơi crawl thì mang theo `data/crawler_state`.

## 8. Xử lý file nhỏ ở tầng raw và clean (2026-11-18)

**Vấn đề:** với 100 game, raw có 1.421 file và `clean/reviews` có 10.737 file Parquet, vì phân vùng theo ngày tạo review trải từ 2013 đến 2026 (PLAN.md mục 3.1.5).

**Chốt:**
1. **Raw:** gộp các file `.jsonl.gz` nhỏ thành một file mỗi `<nguồn>/dt=` bằng `scripts/compact_raw.py` (`make compact`, `python pipeline.py compact`, hoặc `python pipeline.py batch --compact` để gộp rồi chạy batch luôn). Script giữ nguyên layout `dt=YYYY-MM-DD` nên B1/B2 không phải sửa. Chạy sau khi crawl/upload và trước B2.
2. **`clean/reviews` phân vùng theo tháng (`ym=YYYY-MM`) thay vì theo ngày (`dt`).** B2 `repartition("ym")` trước khi ghi nên mỗi tháng một file.
   - **Lý do:** dù có `repartition`, chia theo `dt` vẫn tối thiểu một file mỗi ngày có review, tức vài nghìn file cho 13 năm. Phần lớn ngày cũ chỉ có vài review nên file rất nhỏ. Chia theo tháng còn khoảng 160 file.
   - **Cột `dt` vẫn giữ trong dữ liệu**, nên B3–B7 (đọc cả thư mục rồi lọc/nhóm theo `dt`) không phải sửa. Lọc theo `dt` không còn được partition pruning; muốn cắt nhanh thì lọc thêm `ym`.
   - `clean/ccu` vẫn phân vùng theo `dt` (dữ liệu CCU chỉ có từ lúc bắt đầu crawl, số ngày ít).
   - B2 ghi `clean/reviews` với `partitionOverwriteMode=static`, tức xoá cả thư mục rồi ghi lại. Nếu không, thư mục cũ có `dt=` sẽ nằm lẫn với `ym=` mới và Spark không đọc được.
   - Hive: bảng do `write_table()` tự đăng ký đã nhận `ym`; `storage/hive/create_tables.sql` cũng đã sửa thành `PARTITIONED BY (ym STRING)`.
3. **`curated/review_sentiment` (B5) cũng phân vùng theo `ym`**, `repartition("ym")` và ghi `static`, cùng lý do như mục 2. Cột `dt` vẫn giữ; B3 và notebook 02 chỉ join theo `recommendationid` nên không phải sửa. B9 đọc bảng này.
   - Số đo (2026-11-18, bộ 100 game, `local[4]`): **7.089 → 154 file Parquet**, 68 MB → 2,2 MB; vẫn đủ 34.725 dòng, không trùng `recommendationid`, `ym` khớp `dt` ở mọi dòng; B3 sau đó cho cùng 19.861 dòng `game_daily_stats`.

**Số đo trước/sau (2026-11-18, bộ 100 game, 92.519 bản ghi raw, 87.407 review sau khử trùng):** WSL2 Ubuntu, PySpark 3.5.7, `SPARK_MASTER=local[4]`. "Trước" = B2 ở commit `18bcad1` (phân vùng `dt`), "sau" = B2 hiện tại (`ym`), cùng bộ raw.

| | Trước | Sau | Thay đổi |
| --- | --- | --- | --- |
| Raw: số file `.jsonl.gz` | 1.421 (1.020 review, 200 CCU, 201 meta) | 3 (1 / nguồn / `dt`) | −99,8% |
| Raw: dung lượng | 18,30 MiB | 17,41 MiB | −4,8% |
| `clean/reviews`: số file Parquet | 9.587 | 156 | −98,4% |
| `clean/reviews`: số thư mục phân vùng | 3.723 (`dt=`) | 156 (`ym=`, 2013-09 → 2026-11) | |
| `clean/reviews`: dung lượng | 197 MB | 41 MB | −79% |
| Thời gian B2 (gồm khởi động Spark) | 104 s | 34 s | −67% |

- `compact_raw.py` giữ đủ 92.519 bản ghi; mọi file gzip hợp lệ. Bản gốc lưu ở `data/raw_backup_precompact/` (đã `.gitignore`).
- `clean/reviews` hai bản cùng 87.407 dòng, không lệch `recommendationid` nào; `ym` khớp `dt` ở mọi dòng.
- Mốc 10.737 file trong PLAN.md đo trên máy khác; số file "trước" phụ thuộc số task Spark nên khác với 9.587 ở đây. Thời gian B2 "trước" còn gồm đọc 1.421 file raw, nên phần giảm là tác động chung của cả hai thay đổi.

## 9. Mô hình phân loại cảm xúc B9 (2026-11-19)

**Chốt:** thêm đúng một mô hình ML, `processing/batch/b9_sentiment_classifier.py` (đặc tả: `docs/BigData.md` mục 6 phần phân công). Logistic Regression trên Spark MLlib dự đoán `voted_up` từ nội dung review, so với VADER của B5 trên cùng tập test.

- **Dữ liệu:** review tiếng Anh có nội dung, bỏ review B2 gắn cờ `is_suspicious`. Nhãn là `voted_up`.
- **Đặc trưng:** RegexTokenizer → StopWordsRemover → CountVectorizer → IDF như B5, khác hai điểm: **giữ từ phủ định** (`not`, `no`, `don't`…) và token từ 2 ký tự. Lý do: B5 tìm từ khoá chủ đề nên bỏ được; với phân loại cảm xúc, "not good" và "no" mang nghĩa.
- **Chia train/test 80/20 theo thời gian** (20% review mới nhất làm test); `--split random` (seed 42) để so sánh. Trọng số lớp `n / (2·n_lớp)` vì review tích cực chiếm đa số.
- **Siêu tham số** `minDF=20`, `regParam=0.1`, chọn thô trong 3 cấu hình trên bộ 100 game. `minDF=5` cho top từ toàn từ hiếm và mảnh từ (`ter`, `william`, `temple`).
- **Thứ tự:** chạy cuối batch (sau B8). B9 chỉ cần B5, và là việc bỏ đầu tiên nếu trễ, nên lỗi ở B9 không được chặn các bảng phục vụ dashboard. `--skip b9` để bỏ.
- **Benchmark:** `sc1_sc2_batch_benchmark.py` đo B9 như một job nhưng **không cộng vào `total_pipeline`**, để tổng B2–B8 vẫn so được với các lần đo trước. Chỉ số mô hình ghi vào `batch_ml_scale{N}_{ngày}.csv` và, với `--log-common`, vào mẫu chung (`experiment=ml`, job `b9` và `vader`). `--no-b9` để bỏ.
- **Không ghi Elasticsearch:** kết quả là vài dòng chỉ số, dùng trong notebook và báo cáo, không cần dashboard.

**Kết quả đầu tiên** (2026-11-19, bộ 100 game ngày 12/11, WSL2 `local[4]`; 26.202 review đủ điều kiện, train 20.953 / test 5.249, mốc thời gian 2026-11-07):

| Mô hình | Accuracy | F1 | F1 lớp tiêu cực | AUC | Train | Predict |
| --- | --- | --- | --- | --- | --- | --- |
| Logistic Regression | 0,806 | 0,806 | 0,709 | 0,859 | 12,8 s | 0,8 s |
| VADER (compound ≥ 0,05) | 0,658 | 0,667 | 0,566 | 0,723 | — | — |

- Chia ngẫu nhiên cho số cao hơn (LR AUC 0,887, VADER 0,750), vì test lẫn cùng giai đoạn với train. Báo cáo dùng số chia theo thời gian.
- Đường cong học trong một quy mô (chia ngẫu nhiên, 10% → 100% tập train): AUC 0,810 → 0,862 → 0,875 → 0,887; tăng chậm dần sau khoảng 10 nghìn review.
- Top từ âm: `boring`, `slop`, `worst`, `garbage`, `refund`, `crashing`, `unplayable`, khớp với nhóm từ khoá tiêu cực của B5.
- Đây chưa phải số nộp: số so sánh 100/500/1000 game phải đo trên 3 tập con cắt từ cùng snapshot 1000 game.
