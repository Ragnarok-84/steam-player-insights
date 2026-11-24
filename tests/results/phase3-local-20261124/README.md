# Pilot Phase 3 trên Hyper-V

Đo ngày 24/11/2026 trên `steam-ft`, K3s `v1.34.12+k3s1`. Host Windows có Intel Core i5-12400F, 6 nhân/12 luồng và 32 GB RAM. Giữ 16 GiB cho bốn VM theo RAM trống lúc khởi động. Tăng worker 2 và worker 3 từ 2 lên 4 vCPU. Nhãn compute có trên cả ba worker. Kibana có 0 replica trong lúc đo. [Describe nodes](environment/describe-nodes.txt) · [Số vCPU và RAM VM](environment/hyperv-vms.csv) · [Image và SHA](environment/image-and-host.json)

| VM | vCPU | RAM cấp cho VM |
|---|---:|---:|
| steam-server | 2 | 4 GiB |
| steam-worker-1 | 2 | 3 GiB |
| steam-worker-2 | 4 | 5 GiB |
| steam-worker-3 | 4 | 4 GiB |

Spark dùng `localhost/steam-spark:k3s-20261124-1d45953` từ main `1d459539f58f058a2698f02a6d8bcee5e1565c31`. Cả bốn VM dùng cùng archive đã so SHA256. Các partition Kafka có đủ ba ISR, HDFS có hai DataNode và cả ba tập crawl đều `HEALTHY`. [Trạng thái Kafka và HDFS](environment/steam-ft-status.txt)

| Kiểm tra | Kết quả | Bằng chứng |
|---|---|---|
| 4 executor | 4 pod Ready và 4 executor đăng ký với Spark. Đếm được 192.239 review từ HDFS. | [Pod khi chạy](p3-probe-e4/pods-while-running.txt) · [Log](p3-probe-e4/driver.log) |
| B2–B9, 100 game, 1 executor | PASS. Wall time 315,7 giây; `total_pipeline` từ lúc Spark khởi động đến B8 là 260,516 giây; B9 40,503 giây. | [Pod khi chạy](p3-pilot-batch-e1/pods-while-running.txt) · [Log](p3-pilot-batch-e1/driver.log) · [Check CSV](p3-pilot-batch-e1/results-log-check.txt) |
| B2–B9, 100 game, 2 executor | PASS. Wall time 240,7 giây; `total_pipeline` 194,927 giây; B9 32,739 giây. | [Pod khi chạy](p3-pilot-batch-e2/pods-while-running.txt) · [Log](p3-pilot-batch-e2/driver.log) · [Check CSV](p3-pilot-batch-e2/results-log-check.txt) |
| S1, 100 game, 1 executor | PASS kiểm tra chạy liên tục và thu số đo. Phát 18.000 bản ghi trong 90 giây ở 200 bản ghi/giây. Có 20 micro-batch, lag cuối bằng 0 và p95 end-to-end 16,351 giây. P95 micro-batch 10,132 giây, vượt nhẹ trigger 10 giây, nên công cụ ghi `keeps_up=False`. | [Pod khi chạy](p3-pilot-s1-e1-r2/pods-while-running.txt) · [Tải và số đo](p3-pilot-s1-e1-r2/producer.log) · [Check CSV](p3-pilot-s1-e1-r2/results-log-check.txt) |

Lượt hai executor giảm khoảng 24% wall time. B2 và B5 giảm lần lượt từ 80,594 xuống 55,382 giây và từ 119,676 xuống 87,404 giây. Đây là một lượt mỗi cấu hình. Ma trận dùng ba lượt để lấy trung vị. Batch giữ 16 shuffle partition và mỗi executor dùng một core.

S1 lần đầu dừng vì Elasticsearch trong namespace mới thiếu `realtime_game_metrics`. Sau khi chạy Job tạo mapping chuẩn, lượt S1 mới dùng checkpoint riêng và qua `results_log.py check`. [Log tạo mapping](environment/es-init.log) · [Số document ES sau S1](environment/es-counts-after-s1.json)

Các CSV trong thư mục `results/common` ghi `env=k3s`. Tệp `pod-top.txt` lưu mẫu CPU/RAM thực tế từ Metrics Server. Đỉnh CPU/RAM lịch sử của Phase 2 không được thu lúc chạy; bộ bằng chứng Phase 2 có requests/limits, log, manifest, thời gian, SHA và image ID.
