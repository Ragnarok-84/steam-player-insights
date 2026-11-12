# Các quyết định xây dựng và triển khai

Ghi lại các lựa chọn nhóm đã chốt, kèm lý do, để thành viên mới không phải hỏi lại. Khi đổi một quyết định, sửa mục tương ứng và ghi ngày đổi. Bản thiết kế gốc: `BigData.pdf`. Phân tích tài nguyên: `van_de_va_giai_phap_ha_tang.md`.

Cập nhật lần cuối: 2026-11-12.

## 1. Môi trường làm việc

### 1.1. Phát triển và kiểm chứng logic trên máy cá nhân

**Chốt:** mọi việc viết và sửa job, làm dashboard, gom dữ liệu đều làm trên máy cá nhân, không cần Kubernetes.

- Spark chạy local mode qua `python pipeline.py ...` (profile `local`: raw = `data/raw`, đầu ra = `data/lake/`). Không cần Kafka/HDFS/ES để chạy batch layer.
- Khi cần dịch vụ, dùng `scripts/demo.sh` với `compose.demo.yaml` và **bật từng nhóm một**: `batch` (HDFS), `streaming` (Kafka), `dashboard` (ES + Kibana). Mỗi container giới hạn 1 GB RAM.
- Thử logic trên dữ liệu mẫu trước: `python tests/make_sample_data.py` (có sẵn review bombing, CCU spike, đợt giảm giá).

**Lý do:** đã kiểm chứng ngày 2026-10-26 trên WSL 7,6 GB RAM: B2→B8 và S2 (`--source file`) chạy thành công trên 30 game × 21 ngày. Chỉ bật toàn bộ hệ thống trên K8s mới cần nhiều RAM.

| Việc | Cần bật | RAM ước tính |
| --- | --- | --- |
| Batch layer trên dữ liệu thật | Spark local | 2–3 GB |
| Dashboard Kibana (`pipeline.py batch --es`) | ES + Kibana | 4–5 GB |
| Streaming với Kafka (`replay`, `s1`–`s3`) | Kafka 1 broker + Spark | 3–4 GB |
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

Theo `van_de_va_giai_phap_ha_tang.md`. Đây là cấu hình đang dùng trong `k8s/fault-test`; báo cáo phải ghi rõ các điểm khác thiết kế.

| Thành phần | Thiết kế gốc | Đang dùng | Ghi chú |
| --- | --- | --- | --- |
| Kafka | 3 broker, RF=3, min ISR=2 | Giữ nguyên (KRaft) | Cần cho FT1 có ý nghĩa |
| HDFS | 1 NameNode + 3 DataNode, replication 3 | 1 NameNode + 2 DataNode, replication 2 | Tiết kiệm RAM/đĩa |
| Elasticsearch | 3 node, 1 replica | 1 node, 0 replica | Chưa kiểm thử FT4/SC4 |
| Hive Metastore | Có | Chưa triển khai | Truy vấn ad-hoc bằng Spark SQL trên Parquet (`createOrReplaceTempView`); job tự đăng ký bảng nếu sau này có Metastore |
| Prometheus/Grafana | Có | Chưa triển khai | Tạm dùng `kubectl top` và Kafka consumer lag |
| Quy mô | 1.000–2.000 game, 20–30 GB | 1.000 game (tối đa 2.000), mục tiêu 10–20 GB | Cần ≥ 14 ngày dữ liệu cho B6/B7 |

## 3. Quy ước code và vận hành

- **Thứ tự batch:** B1 → B2 → B5 → B3 → B4 → B6 → B7 → B8, rồi `merge_views` khi ghi ES. B3 dùng cảm xúc từ B5; B7 tạo baseline cho S2/S3. `Makefile` và `pipeline.py` dùng cùng thứ tự này.
- **Layout dữ liệu:** raw `…/raw/{steam_reviews_raw|steam_players_ccu|steam_games_meta}/dt=YYYY-MM-DD/*.jsonl.gz`. Crawler local và B1 ghi cùng layout, nên dữ liệu crawl dùng trực tiếp làm raw.
- **Tham số job:** `--raw`, `--clean`, `--curated` là **thư mục gốc**, không trỏ tới thư mục con của từng nguồn.
- **Chạy lại an toàn:** B1 lưu offset; B2–B8 tính lại từ dữ liệu nguồn và ghi đè. Không cần đổi thư mục đầu ra khi chạy lại.
- **Ghi Elasticsearch:** dùng Python client (`processing/common/es_writer.py`), không dùng connector JVM. Bật bằng `WRITE_ES=1` hoặc `--es-nodes`. Mapping chuẩn ở `storage/elasticsearch/indices_mapping.json`; mapping demo (`infra/demo/elasticsearch-indices.json`) sinh từ mapping chuẩn với 1 shard / 0 replica.
- **Image Spark cho k3s:** image chứa bản sao code tại thời điểm build. **Mỗi khi `processing/` thay đổi phải build lại và tăng tag** (dạng `k3s-YYYYMMDD`), sửa tag ở mọi manifest. Tag hiện tại: `localhost/steam-spark-demo:k3s-20261026`.
- **Phân tích văn bản:** chỉ phân tích nội dung review tiếng Anh (VADER, từ điển khía cạnh, TF-IDF); ngôn ngữ khác chỉ dùng nhãn `voted_up`.

## 4. Thứ tự công việc đã thống nhất

1. **Máy cá nhân:** chạy pipeline trên dữ liệu thật và sửa lỗi; dựng dashboard Kibana D1–D4 và export saved objects (ndjson); cho crawler chạy hằng ngày, backfill dữ liệu lịch sử.
2. **Trước khi bật cloud:** viết manifest Job/Deployment trên k3s cho B1–B8 và S1–S3 (hiện mới có B2 và smoke), để thời gian trên cloud chỉ dùng để chạy.
3. **Trên cloud, theo buổi:** dựng cụm 4 VM, chạy trọn pipeline, chạy FT/SC theo `infra/k3s/FAULT_TEST.md`, lưu kết quả vào `tests/results/`, chụp màn hình và quay video. Tắt VM sau mỗi buổi.
4. **Bàn giao:** notebook, `docs/architecture.md`, trả lời 5 câu hỏi §1.3 bằng số liệu, báo cáo, slide, video.

## 5. Chưa chốt, cần hỏi giảng viên

Theo mục 6 của `van_de_va_giai_phap_ha_tang.md`:

1. Bỏ Hive Metastore, dùng Spark SQL đọc trực tiếp Parquet có được chấp nhận không?
2. Cụm k3s trên VM cloud và máy cá nhân nối qua VPN có được xem là "mô phỏng môi trường phân tán" không?
3. "Không dùng Docker" có cấm cả container runtime trong K8s (containerd của k3s) và Compose dùng để thử local không?
4. Quy mô 1.000–2.000 game và 10–20 GB dữ liệu có đủ không?
5. Giảm HDFS replication 3 → 2 và Elasticsearch 3 → 1 node có ảnh hưởng điểm phần kiểm thử chịu lỗi không?

## 6. Nguồn thu thập dữ liệu (2026-11-12)

**Chốt:** chỉ thu thập trực tiếp qua API Steam, dùng cấu hình `.env`. Không dùng Hugging Face, Kaggle hay SteamSpy, kể cả để chọn danh sách game. Review lịch sử lấy bằng phân trang API Steam. Mỗi game có cùng hạn mức số trang trong một lượt; không bù số lượng bằng dataset ngoài.

**Cập nhật cách chọn game:** lấy mẫu đều trên toàn catalog cho ~70% game có dưới 20 review và ~29% game chưa phát hành (lần thử 30 game: 13 game không có review, 11 game lỗi CCU). Danh sách mới gồm nhóm `top_ccu` (bảng most-played chính thức của Steam) và nhóm `stratified` (mẫu có seed, đã lọc game đã phát hành và ≥ 20 review, chia đều 4 tầng số review). Ghi rõ trong báo cáo: nhóm `top_ccu` là game phổ biến, nhóm `stratified` không đại diện cho game dưới ngưỡng. Danh sách chỉ được mở rộng, không xáo lại; review crawl tăng dần nên lần thu lớn sau dùng lại toàn bộ dữ liệu đã có. Tag lấy từ `IStoreBrowseService/GetItems`, số review theo ngày từ `appreviewhistogram` thay cho SteamSpy. Raw/lake cũ trộn nguồn được lưu riêng trong `data/legacy_nonsteam/`, không dùng cho kết quả mới.
