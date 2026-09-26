import subprocess
import time
import argparse
import csv

def run_spark_benchmark(job_path: str, executor_counts: list, data_scales: list, output_csv: str = "scalability_results.csv"):
    """
    Kịch bản SC1 & SC2: Kiểm thử khả năng mở rộng Batch Processing
    - SC1: Cố định dữ liệu, tăng executor (1, 2, 4, 6)
    - SC2: Cố định executor, tăng dữ liệu (1x, 2x, 5x, 10x)
    """
    results = []

    for scale in data_scales:
        for exec_num in executor_counts:
            print(f"\n==================================================")
            print(f"Running Benchmark: Data Scale = {scale}x | Spark Executors = {exec_num}")
            print(f"==================================================")

            start_time = time.time()
            cmd = [
                "spark-submit",
                "--master", "k8s://https://kubernetes.default.svc",
                f"--conf", f"spark.executor.instances={exec_num}",
                "--conf", "spark.executor.memory=2g",
                "--conf", "spark.executor.cores=1",
                job_path,
                "--scale", str(scale)
            ]

            print(f"Executing: {' '.join(cmd)}")
            try:
                # Giả lập chạy hoặc thực thi thực tế khi submit
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
                elapsed = time.time() - start_time
                status = "SUCCESS" if res.returncode == 0 else "FAILED"
            except Exception as e:
                elapsed = time.time() - start_time
                status = f"ERROR: {str(e)}"

            results.append({
                "job": job_path,
                "data_scale": f"{scale}x",
                "executors": exec_num,
                "elapsed_seconds": round(elapsed, 2),
                "status": status
            })

    # Ghi kết quả benchmark ra file CSV để phục vụ báo cáo
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["job", "data_scale", "executors", "elapsed_seconds", "status"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nBenchmark completed. Results written to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SC1 & SC2 Scalability Benchmark")
    parser.add_argument("--job", default="processing/batch/b2_clean_raw.py", help="Spark job script")
    parser.add_argument("--executors", nargs="+", type=int, default=[1, 2, 4, 6], help="Executor counts")
    parser.add_argument("--scales", nargs="+", type=int, default=[1, 2, 5], help="Data scale multiples")
    args = parser.parse_args()

    run_spark_benchmark(args.job, args.executors, args.scales)
