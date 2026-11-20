# Benchmark batch B2–B9 theo quy mô 100 / 500 / 1000 game (local, 20/11/2026)

Người đo: Mạnh Hùng (thành viên B). Giai đoạn 2 của phần so sánh quy mô (`docs/BigData.md`, mục 2–4 phần phân công). Đây là số đo **một máy**; giai đoạn 3 đo lại trên cụm k3s.

## 1. Cách đo

| Mục | Giá trị |
| --- | --- |
| Dữ liệu | Gói `steam_1000g_20261118` của Hải Long (crawl 12/11 và 17–18/11), cắt bằng `tests/scalability/make_scale_subsets.py` thành 3 tập lồng nhau: 100, 500, 1000 game đầu `target_apps.json`. Ba tập cùng khoảng thời gian thu; không bản ghi nào bị bỏ |
| Máy | WSL2 Ubuntu, 16 luồng CPU, 9 GB RAM; Python 3.11, PySpark 3.5.7, OpenJDK 11 |
| Đĩa | Raw và lake đặt trên ext4 của WSL (`~/steam-bench`), không qua `/mnt/d`, để không đo tốc độ chia sẻ file Windows |
| Spark | `local[N]`, N = số "executor" ∈ {1, 2, 4}; `SPARK_DRIVER_MEMORY=5g` cho mọi lượt; `spark.sql.shuffle.partitions = max(4, 4N)` |
| Lặp | 3 lần mỗi cấu hình, báo cáo **trung vị**; 27/27 lượt thành công |
| Script | `sc1_sc2_batch_benchmark.py --scales 100 500 1000 --executors 1 2 4 --runs 3 --log-common`, commit `c4be999` |
| `total_pipeline` | Khởi động Spark + B2…B8. **B9 đo riêng, không cộng vào tổng** |

Kết quả gốc: `tests/results/batch_raw_scale{N}_{,details_,summary_}20261120.csv`, `batch_ml_scale{N}_20261120.csv`; mẫu chung: `tests/results/common/{batch,ml,storage}_{N}_local_20261120_B.csv`.

### Quy mô dữ liệu

| | 100 game | 500 game | 1000 game |
| --- | --- | --- | --- |
| Review raw | 192.239 | 619.514 | 932.293 |
| Tổng bản ghi clean sau B2 (mọi bảng) | 187.079 | 611.437 | 928.860 |
| CCU / metadata | 400 / 900 | 1.200 / 3.300 | 1.700 / 4.800 |

## 2. Thời gian theo job (trung vị, giây)

| Job | 100 · e1 | 100 · e4 | 500 · e1 | 500 · e4 | 1000 · e1 | 1000 · e2 | 1000 · e4 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Khởi động Spark | 3,0 | 3,4 | 3,4 | 3,3 | 3,3 | 3,3 | 3,8 |
| B2 làm sạch | 44,2 | 27,7 | 118,2 | 64,9 | 161,4 | 107,9 | 103,7 |
| B5 VADER, khía cạnh, TF-IDF | 67,2 | 53,5 | 236,8 | 120,7 | 347,9 | 231,1 | 168,5 |
| B3 thống kê | 12,1 | 9,0 | 28,2 | 17,9 | 38,7 | 29,9 | 24,8 |
| B4 giờ chơi | 1,9 | 1,4 | 2,9 | 1,9 | 3,5 | 2,9 | 2,2 |
| B6 trending | 4,9 | 4,4 | 8,1 | 7,5 | 10,4 | 10,0 | 9,9 |
| B7 baseline | 2,4 | 2,0 | 2,7 | 1,9 | 3,5 | 2,9 | 2,5 |
| B8 K-Means | 3,6 | 4,0 | 4,1 | 4,4 | 4,2 | 4,8 | 5,2 |
| **Tổng B2–B8** | **138,5** | **105,7** | **401,1** | **221,2** | **575,5** | **401,3** | **323,3** |
| B9 Logistic Regression (riêng) | 25,0 | 19,2 | 61,5 | 38,7 | 86,3 | 64,3 | 59,2 |

## 3. Speedup theo số executor (SC1)

Speedup = trung vị(e=1) / trung vị(e=N), tính trên `total_pipeline`.

| Quy mô | e=2 | e=4 |
| --- | --- | --- |
| 100 game | 1,24× | 1,31× |
| 500 game | 1,56× | 1,81× |
| 1000 game | 1,43× | 1,78× |

Ở 1000 game, speedup e=4 của từng job: B5 2,06×, B2 1,56×, B3 1,56×, B9 1,46×; B6 1,05×, B8 0,81× (chậm hơn).

## 4. Thời gian theo quy mô (SC2)

Với e=4: 105,7 s → 221,2 s → 323,3 s, trong khi số review tăng 1× → 3,2× → 4,8×. Thời gian tăng 2,1× và 3,1×, tức **dưới tuyến tính**: phần chi phí cố định (khởi động, job nhỏ B4/B6/B7/B8, ghi Parquet) được chia trên nhiều dữ liệu hơn. Tính trên review: 0,55 ms/review ở 100 game, 0,36 ms ở 500, 0,35 ms ở 1000 (e=4) — từ 500 game trở lên chi phí trên mỗi review gần như không đổi, tức phần biến đổi tăng tuyến tính.

## 5. Lưu trữ

Số đo lấy từ lake của lượt cuối (e=4, run 3), Parquet snappy.

| | 100 game | 500 game | 1000 game |
| --- | --- | --- | --- |
| Raw (đã gộp, 1 file / nguồn / ngày) | 35,3 MiB · 6 file | 125,9 MiB · 7 file | 193,0 MiB · 7 file |
| Clean | 79,4 MiB · 163 file | 285,9 MiB · 194 file | 439,7 MiB · 198 file |
| Curated | 3,0 MiB · 184 file | 10,7 MiB · 229 file | 17,2 MiB · 233 file |

- `clean/reviews` chiếm gần hết clean; lớn hơn raw gzip vì giữ cả văn bản gốc, văn bản đã làm sạch và `text_hash`.
- Số file chỉ tăng từ 163 lên 198 khi dữ liệu tăng 5,5×, nhờ phân vùng theo tháng (`ym`) và `repartition("ym")` (`docs/decisions.md`, mục 8). Khoảng 190 file là số tháng có review (2010–2026), không phụ thuộc số game.
- Curated nhỏ: phần lớn là bảng tổng hợp; `review_sentiment` 1,2 → 5,1 MiB.

## 6. Tài nguyên

| Quy mô | RAM đỉnh e1 / e2 / e4 (MB) | CPU đỉnh e1 / e2 / e4 (%) |
| --- | --- | --- |
| 100 game | 3.038 / 3.298 / 3.811 | 790 / 995 / 1.366 |
| 500 game | 3.534 / 3.463 / 3.892 | 910 / 1.106 / 1.387 |
| 1000 game | 3.732 / 3.656 / 3.846 | 686 / 948 / 1.357 |

RAM và CPU đo trên cây tiến trình driver (JVM + Python worker), lấy mẫu mỗi 0,5 s; CPU cộng mọi lõi nên vượt 100%. RAM đỉnh gần như không đổi theo quy mô (khoảng 3,0–3,9 GB) vì bị giới hạn bởi heap 5 GB của driver chứ không phải lượng dữ liệu.

## 7. Mô hình B9 theo quy mô

Logistic Regression (MLlib, `minDF=20`, `regParam=0.1`), chia theo thời gian 80/20; VADER chấm trên cùng tập test. Chỉ số giống nhau giữa các lượt (seed cố định), bảng ghi trung vị của 9 lượt.

| Quy mô | Train / test | % tích cực (test) | Mô hình | Accuracy | F1 | F1 lớp tiêu cực | AUC |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 43.658 / 10.929 | 69,0 | LogReg | 0,821 | 0,822 | 0,715 | 0,874 |
| | | | VADER | 0,668 | 0,679 | 0,558 | 0,734 |
| 500 | 156.797 / 39.154 | 71,3 | LogReg | 0,852 | 0,851 | 0,737 | 0,897 |
| | | | VADER | 0,688 | 0,700 | 0,547 | 0,741 |
| 1000 | 237.747 / 59.435 | 74,4 | LogReg | 0,860 | 0,860 | 0,727 | 0,901 |
| | | | VADER | 0,700 | 0,715 | 0,526 | 0,747 |

Thời gian (trung vị, giây; e1 / e2 / e4):

| Quy mô | train_s | predict_s |
| --- | --- | --- |
| 100 | 16,3 / 12,3 / 11,7 | 0,86 / 0,80 / 0,78 |
| 500 | 45,7 / 30,9 / 27,7 | 2,20 / 2,19 / 2,23 |
| 1000 | 66,7 / 47,6 / 44,0 | 3,20 / 3,30 / 3,56 |

- **Đường cong học:** AUC 0,874 → 0,897 → 0,901; tăng rõ từ 100 lên 500 game, gần bão hoà sau đó. VADER gần như đứng yên (0,734 → 0,747) vì không học từ dữ liệu.
- LogReg hơn VADER 15–16 điểm accuracy và 0,14–0,16 AUC ở mọi quy mô; khoảng cách F1 lớp tiêu cực 0,16–0,20.
- `train_s` tăng gần tuyến tính theo số review train (43,7k → 237,7k review: 5,4× dữ liệu, 4× thời gian ở e1). `predict_s` nhỏ và không tăng tốc theo executor (tập test chỉ vài partition).
- Top từ âm (1000 game): `boring`, `abandoned`, `unplayable`, `abandonware`, `refunded`, `refund`, `waste`, `worst`, `ruined`, `crashes`, `uninstalled`; 21/30 từ âm nằm trong top từ khoá tiêu cực TF-IDF của B5 ở ít nhất một game. Top từ dương: `great`, `amazing`, `fun`, `underrated`, `best`, `addicting`; có vài từ lóng/meme (`shrimp`, `goated`, `goooood`) vì Steam có nhiều review đùa.

## 8. Nhận xét

1. **Khởi động Spark không phải nút thắt** trong benchmark: khoảng 3 s, chiếm 2–3% tổng ở 100 game và 0,6–1,2% ở 1000 game. Nhận xét cũ "thời gian khởi động chiếm phần lớn" (chạy một máy, 11.637 review) đến từ `pipeline.py batch` mở 7 SparkSession riêng và dữ liệu quá nhỏ; benchmark dùng một session.
2. **Nút thắt là B5 (VADER, Python UDF) và B2**, cộng lại 77–89% tổng thời gian (tỷ trọng tăng theo quy mô). B5 cũng là job tăng tốc tốt nhất (2,06× ở e=4, 1000 game) vì tính trên từng review, không cần shuffle.
3. **Speedup thấp ở 100 game** (1,31×): dữ liệu nhỏ, phần lớn job ngắn (B4/B6/B7/B8 vài giây) bị chi phí lập lịch và ghi file chi phối. Từ 500 game speedup ở e=4 đạt 1,8×.
4. **Speedup e=4 còn xa 4×** vì đây là một máy: `local[4]` chia CPU/RAM/đĩa với driver và Python worker, CPU đỉnh tăng từ 686% (e=1) lên 1.357% (e=4) ở 1000 game. B8 (K-Means trên ~1.000 dòng) chậm đi khi tăng executor, đúng kỳ vọng với dữ liệu rất nhỏ.
5. **Dữ liệu tăng 4,8× thì thời gian tăng 3,1×** (e=4). Với chi phí trên review ổn định từ 500 game, nếu 2.000 game cho khoảng 1,9 triệu review thì ước lượng bậc nhất khoảng 650 s ở e=4 trên máy này.

## 9. Giới hạn

- `local[N]` chỉ giới hạn số task chạy song song, không giới hạn số lõi CPU của JVM (GC, JIT) và Python worker; e=1 vì thế dùng tới 7–9 lõi lúc đỉnh. Số "executor" ở đây không tương đương executor trên cụm.
- Một máy, không có mạng và HDFS: không đo được chi phí shuffle qua mạng. Phải đo lại trên cụm k3s (giai đoạn 3).
- CCU mới có 1–2 snapshot mỗi game, nên B6/B7 xử lý ít dữ liệu CCU; thời gian của hai job này sẽ tăng khi có ≥ 14 ngày CCU.
- Siêu tham số B9 chọn thô trên bộ 100 game ngày 12/11 (`docs/decisions.md`, mục 9), không tinh chỉnh lại cho bộ mới.

## 10. Cách chạy lại

```bash
python tests/scalability/make_scale_subsets.py --raw data/incoming/data-1000g/raw \
    --targets data/incoming/data-1000g/target_apps.json --out ~/steam-bench/scale --dataset steam_1000g_20261118
SPARK_DRIVER_MEMORY=5g python tests/scalability/sc1_sc2_batch_benchmark.py --scales 100 500 1000 \
    --executors 1 2 4 --runs 3 --raw-template ~/steam-bench/scale/raw_{scale} \
    --lake-template ~/steam-bench/lake/lake_{scale} --log-dir ~/steam-bench/logs \
    --topology wsl2-16c-9g-ext4 --dataset steam_1000g_20261118 --log-common --member B
python tests/scalability/storage_stats.py --raw-template ~/steam-bench/scale/raw_{scale} \
    --lake-template ~/steam-bench/lake/lake_{scale} --dataset steam_1000g_20261118 --log-common
```

Toàn bộ ma trận (27 lượt) mất khoảng 2,5 giờ trên máy trên.
