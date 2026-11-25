# Kết quả Phase 3 trên Hyper-V

Hàng đợi hoàn tất lúc **18:16 ngày 25/11/2026 (UTC+7)**. Cụm đo gồm bốn VM Ubuntu trên một host Hyper-V, namespace chính là `steam-ft`. [Tiến trình chạy](environment/workflow-status-final.txt) · [Nodes sau kiểm thử](environment/nodes-after-ft.txt) · [Pods sau kiểm thử](environment/pods-after-ft.txt)

## Ma trận hiệu năng

Batch B2–B9 đo ở 100, 500 và 1.000 game với 1, 2 và 4 executor, mỗi cấu hình ba lần. Mỗi executor có một core. **8/9 cấu hình PASS với giới hạn 1,5 GiB/executor ban đầu.** Cấu hình 1.000 game với 2 executor hoàn tất B2–B8 cả ba lần. B9 lỗi OOM ở lần 2 và 3. Hai lần B9 chạy lại trên cùng đầu vào đều PASS với giới hạn 1,75 GiB/executor (heap 1 GiB, overhead 768 MiB). Hai lượt này được ghi riêng khỏi ma trận RAM ban đầu. [Cấu hình gốc và log lỗi](p3-batch-1000-e2) · [B9 chạy lại lần 2](p3-b9-1000-e2-r2-retry) · [B9 chạy lại lần 3](p3-b9-1000-e2-r3-retry)

**81/81 lượt streaming S1–S3 có dữ liệu đo hợp lệ**, gồm 27 lượt mỗi job. Mỗi lượt có hai bậc tải mục tiêu 1.000 và 5.000 bản ghi/giây. Các thư mục `p3-s1-*`, `p3-s2-*` và `p3-s3-*` chứa manifest, log và CSV có `env=k3s`. Cột `keeps_up` cho biết job có theo kịp từng bậc tải. Lượt S1 ở 100 game, 2 executor, lần 2 từng hết hạn ở bộ phát tải và đã qua kiểm tra khi chạy lại. [Lượt hết hạn](p3-s1-100-e2-r2) · [Lượt chạy lại](p3-s1-100-e2-r2-a2)

## Kiểm thử chịu lỗi

| Bài | Kết quả quan sát | Bằng chứng |
|---|---|---|
| FT1 | Đọc lại đủ 293 bản tin Kafka đã xác nhận sau khi mất broker. | [Log observer](ft1/observer.log) |
| FT2 | HDFS có 3 lỗi đọc khi thay DataNode, sau đó trở lại đủ 2 bản sao. Kafka giữ đủ 293 bản tin đã xác nhận. | [Log observer](ft2/observer.log) · [HDFS fsck](ft2/fsck-after.log) |
| FT3 | Spark tạo executor thay thế và đối chiếu ID review không thiếu hay trùng. S1 phục hồi từ checkpoint sau khi thay driver và cập nhật tổng từ 10 lên 25 sự kiện. | [Executor và đối chiếu ID](ft3a) · [Driver và checkpoint](ft3b) |
| FT5 | Worker-1 chuyển `NotReady`, sau đó phục hồi khi cập nhật địa chỉ IP mới mà Hyper-V cấp. Kafka giữ đủ 137 bản tin đã xác nhận. HDFS ghi nhận 34 lỗi đọc trong giai đoạn sự cố, rồi trở lại đủ 2 bản sao. | [Nhật ký nguồn VM](ft5/vm-power.txt) · [Log observer](ft5/observer.log) · [Cập nhật IP](ft5/node-ip-recovery.txt) |
| FT4 | Elasticsearch riêng vẫn đọc được đủ 1.200 tài liệu khi mất một data node. Cụm trở lại `green` với 3 node, 2 data node và đủ 2 shard hoạt động. Spark đã dừng trong suốt bài kiểm thử. | [Mốc sự kiện](ft4/actions.txt) · [Trạng thái sau phục hồi](ft4/recovered-health.json) |

## Pilot 100 game

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
