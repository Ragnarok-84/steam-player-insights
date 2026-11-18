#!/usr/bin/env python3
"""
compact_raw.py
==============
Script gộp các file nhỏ .jsonl.gz trong tầng raw (data/raw) theo từng ngày (dt=YYYY-MM-DD)
và từng nguồn dữ liệu (steam_reviews_raw, steam_players_ccu, steam_games_meta...).

Mục tiêu:
  - Khắc phục vấn đề phân mảnh file nhỏ (small files problem) gây nghẽn I/O khi nạp vào HDFS / PySpark.
  - Bảo toàn 100% dữ liệu: kiểm tra định dạng JSON, đối soát số dòng, kiểm tra checksum CRC32.
  - Giữ nguyên cấu trúc thư mục <raw-dir>/<source>/dt=YYYY-MM-DD/ để job Clean (b2_clean_raw.py)
    đọc bình thường mà không cần sửa code.
  - Hỗ trợ Idempotent (chạy lại an toàn), Dry-run, Backup, và giới hạn số bản ghi trên mỗi file.

Chỉ sử dụng thư viện chuẩn của Python (os, sys, gzip, json, argparse, shutil, pathlib, tempfile...).
"""

import argparse
import gzip
import json
import math
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gộp các file nhỏ .jsonl.gz trong tầng data/raw theo ngày và nguồn dữ liệu.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Đường dẫn thư mục raw chứa các nguồn dữ liệu (mặc định: data/raw).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chế độ chạy thử: quét, kiểm tra, đếm bản ghi và in dự kiến kết quả mà không ghi/xóa file.",
    )
    parser.add_argument(
        "--backup-dir",
        default=None,
        help="Đường dẫn thư mục sao lưu dữ liệu gốc trước khi gộp (tùy chọn).",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=0,
        help="Số bản ghi tối đa trên mỗi file sau gộp (mặc định: 0 = gộp tất cả thành 1 file part-00000.jsonl.gz).",
    )
    return parser.parse_args()


def format_bytes(num_bytes: int) -> float:
    """Chuyển đổi số byte sang MiB (1 MiB = 1024 * 1024 bytes)."""
    return num_bytes / (1024 * 1024)


def is_source_directory(path: Path) -> bool:
    """
    Kiểm tra xem một đường dẫn có phải là thư mục nguồn hay không.
    Thư mục nguồn phải là thư mục và chứa ít nhất một thư mục con phân vùng dạng dt=YYYY-MM-DD.
    Các file báo cáo JSON (như collection-worker-*.json) và các thư mục khác sẽ bị bỏ qua.
    """
    if not path.is_dir():
        return False
    for child in path.iterdir():
        if child.is_dir() and child.name.startswith("dt="):
            return True
    return False


def get_dt_partitions(source_dir: Path) -> List[Path]:
    """Lấy danh sách các thư mục phân vùng dt=YYYY-MM-DD đã sắp xếp."""
    partitions = [
        d for d in source_dir.iterdir()
        if d.is_dir() and d.name.startswith("dt=")
    ]
    return sorted(partitions, key=lambda p: p.name)


def get_partition_gz_files(dt_dir: Path) -> List[Path]:
    """Lấy danh sách các file .jsonl.gz hợp lệ (bỏ qua file ẩn và file .tmp)."""
    files = [
        f for f in dt_dir.iterdir()
        if f.is_file()
        and f.name.endswith(".jsonl.gz")
        and not f.name.startswith(".")
        and ".tmp" not in f.name
    ]
    return sorted(files, key=lambda f: f.name)


def backup_raw_data(raw_dir: Path, backup_dir: Path):
    """Sao lưu toàn bộ thư mục raw sang backup_dir."""
    print(f"\n[BACKUP] Bắt đầu sao lưu từ '{raw_dir}' sang '{backup_dir}'...")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Sao chép toàn bộ cây thư mục raw
    shutil.copytree(raw_dir, backup_dir, dirs_exist_ok=True)
    print(f"[BACKUP] Hoàn tất sao lưu an toàn sang '{backup_dir}'.\n")


def compact_partition(
    dt_dir: Path,
    max_records_per_file: int = 0,
    dry_run: bool = False,
) -> Tuple[int, int, int, int, int]:
    """
    Gộp các file nhỏ trong một thư mục dt=.
    
    Trả về:
      (before_files, before_records, before_bytes, after_files, after_bytes)
    """
    files = get_partition_gz_files(dt_dir)
    if not files:
        return 0, 0, 0, 0, 0

    # Dọn dẹp các file tạm sót lại từ các lần chạy trước bị crash hoặc ngắt quãng (nếu có)
    if not dry_run:
        for leftover in dt_dir.iterdir():
            if leftover.is_file() and (".tmp_" in leftover.name or leftover.name.endswith(".tmp")):
                try:
                    leftover.unlink()
                except Exception:
                    pass

    # Tính chất Idempotent:
    # Nếu chỉ có duy nhất 1 file và tên là part-00000.jsonl.gz (và không yêu cầu chia nhỏ theo max_records),
    # bỏ qua không xử lý lại.
    if max_records_per_file <= 0 and len(files) == 1 and files[0].name == "part-00000.jsonl.gz":
        file_size = files[0].stat().st_size
        record_count = 0
        with gzip.open(files[0], "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    record_count += 1
        print(f"  [-] {dt_dir.name}: Đã ở dạng chuẩn 'part-00000.jsonl.gz' ({record_count} bản ghi). Bỏ qua.")
        return 1, record_count, file_size, 1, file_size

    before_files = len(files)
    before_bytes = sum(f.stat().st_size for f in files)

    if dry_run:
        # Chế độ chạy thử: chỉ đếm bản ghi và kiểm tra JSON
        read_records = 0
        for f in files:
            with gzip.open(f, "rt", encoding="utf-8") as gz:
                for line_idx, line in enumerate(gz, 1):
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        json.loads(s)
                    except Exception as e:
                        raise ValueError(f"Dòng JSON không hợp lệ tại {f.name}:{line_idx}: {e}")
                    read_records += 1

        if max_records_per_file > 0 and read_records > 0:
            projected_files = math.ceil(read_records / max_records_per_file)
        else:
            projected_files = 1 if read_records > 0 else 0

        print(
            f"  [DRY-RUN] {dt_dir.name}: {before_files} file ({read_records:,d} bản ghi) "
            f"-> Dự kiến {projected_files} file 'part-*.jsonl.gz'"
        )
        return before_files, read_records, before_bytes, projected_files, before_bytes

    # Chế độ thực thi: Ghi ra file tạm và đối soát an toàn
    read_records = 0
    written_records = 0
    records_in_curr_part = 0
    part_idx = 0
    pid = os.getpid()

    temp_parts: List[Tuple[Path, Path]] = []  # (temp_path, final_path)
    curr_writer = None

    def open_part(idx: int) -> Tuple[gzip.GzipFile, Path, Path]:
        final_name = f"part-{idx:05d}.jsonl.gz"
        temp_name = f".{final_name}.tmp_{pid}_{int(time.time()*1000)}"
        t_path = dt_dir / temp_name
        fn_path = dt_dir / final_name
        gz_out = gzip.open(t_path, "wt", encoding="utf-8", compresslevel=6)
        return gz_out, t_path, fn_path

    try:
        curr_writer, curr_t_path, curr_fn_path = open_part(part_idx)
        temp_parts.append((curr_t_path, curr_fn_path))

        for file_path in files:
            with gzip.open(file_path, "rt", encoding="utf-8") as in_gz:
                for line_num, raw_line in enumerate(in_gz, 1):
                    line_str = raw_line.strip()
                    if not line_str:
                        continue  # Bỏ qua dòng trống

                    # Kiểm tra JSON hợp lệ
                    try:
                        json.loads(line_str)
                    except Exception as err:
                        raise ValueError(
                            f"Lỗi cú pháp JSON trong file '{file_path.name}' dòng {line_num}: {err}"
                        )

                    read_records += 1

                    # Cắt file nếu vượt quá max_records_per_file
                    if max_records_per_file > 0 and records_in_curr_part >= max_records_per_file:
                        curr_writer.close()
                        part_idx += 1
                        records_in_curr_part = 0
                        curr_writer, curr_t_path, curr_fn_path = open_part(part_idx)
                        temp_parts.append((curr_t_path, curr_fn_path))

                    curr_writer.write(line_str + "\n")
                    records_in_curr_part += 1
                    written_records += 1

        # Đóng writer cuối cùng
        if curr_writer is not None:
            curr_writer.close()
            curr_writer = None

        # ------------------------------------------------------------------
        # ĐỐI SOÁT TOÀN VẸN DỮ LIỆU (DATA INTEGRITY RECONCILIATION)
        # ------------------------------------------------------------------
        if read_records != written_records:
            raise RuntimeError(
                f"LỆCH SỐ BẢN GHI tại {dt_dir}: đọc {read_records} != ghi {written_records}"
            )

        # Kiểm tra đọc lại toàn bộ file tạm để xác thực giải nén & mã kiểm tra CRC32
        verified_records = 0
        for t_path, _ in temp_parts:
            part_verified = 0
            with gzip.open(t_path, "rt", encoding="utf-8") as verify_gz:
                for v_line in verify_gz:
                    if v_line.strip():
                        part_verified += 1
            verified_records += part_verified

        if verified_records != read_records:
            raise RuntimeError(
                f"LỖI TOÀN VẸN CRC/DÒNG tại {dt_dir}: xác thực {verified_records} != đọc {read_records}"
            )

        # ------------------------------------------------------------------
        # HOÀN TẤT: ĐỔI TÊN NGUYÊN TỬ VÀ DỌN DẸP FILE CŨ
        # ------------------------------------------------------------------
        final_targets = [fn for _, fn in temp_parts]
        final_targets_set = set(final_targets)

        # 1. Đổi tên các file tạm sang file chính thức (part-00000.jsonl.gz...)
        for t_path, fn_path in temp_parts:
            os.replace(t_path, fn_path)

        # 2. Xóa các file nhỏ cũ (chỉ xóa những file không trùng với file vừa tạo mới)
        for old_f in files:
            if old_f not in final_targets_set and old_f.exists():
                old_f.unlink()

        after_files = len(final_targets)
        after_bytes = sum(fn.stat().st_size for fn in final_targets if fn.exists())

        print(
            f"  [V] {dt_dir.name}: {before_files} file ({read_records:,d} bản ghi) "
            f"-> {after_files} file part-*.jsonl.gz "
            f"({format_bytes(before_bytes):.2f} MiB -> {format_bytes(after_bytes):.2f} MiB)"
        )
        return before_files, read_records, before_bytes, after_files, after_bytes

    except Exception as exc:
        # Xảy ra sự cố: Dọn dẹp sạch toàn bộ file tạm, giữ nguyên 100% dữ liệu gốc
        if curr_writer is not None:
            try:
                curr_writer.close()
            except Exception:
                pass
        for t_path, _ in temp_parts:
            if t_path.exists():
                try:
                    t_path.unlink()
                except Exception:
                    pass
        raise exc


def print_summary_table(
    summary_data: Dict[str, Dict[str, any]],
    dry_run: bool = False,
):
    """In bảng thống kê so sánh Trước khi gộp vs Sau khi gộp."""
    title_suffix = " (DỰ KIẾN - DRY RUN)" if dry_run else ""
    header_title = f"BÁO CÁO TỔNG HỢP GỘP FILE RAW (RAW COMPACTION SUMMARY){title_suffix}"
    
    line_separator = "=" * 106
    sub_separator = "-" * 106

    print("\n" + line_separator)
    print(header_title.center(106))
    print(line_separator)
    print(
        f"{'Nguồn dữ liệu':<22} | {'Trạng thái':<16} | {'Số dt=':>7} | "
        f"{'Số file .gz':>12} | {'Số bản ghi':>14} | {'Dung lượng (MiB)':>18}"
    )
    print(sub_separator)

    total_dt_before = 0
    total_dt_after = 0
    total_files_before = 0
    total_files_after = 0
    total_records_before = 0
    total_records_after = 0
    total_bytes_before = 0
    total_bytes_after = 0

    for source_name, stats in summary_data.items():
        b_dt = stats["before_dt"]
        a_dt = stats["after_dt"]
        b_files = stats["before_files"]
        a_files = stats["after_files"]
        b_rec = stats["before_records"]
        a_rec = stats["after_records"]
        b_bytes = stats["before_bytes"]
        a_bytes = stats["after_bytes"]

        total_dt_before += b_dt
        total_dt_after += a_dt
        total_files_before += b_files
        total_files_after += a_files
        total_records_before += b_rec
        total_records_after += a_rec
        total_bytes_before += b_bytes
        total_bytes_after += a_bytes

        b_mib = format_bytes(b_bytes)
        a_mib = format_bytes(a_bytes) if not dry_run else b_mib

        # Tính toán mức giảm
        diff_files = a_files - b_files
        pct_files = (diff_files / b_files * 100) if b_files > 0 else 0.0
        diff_bytes = (a_bytes - b_bytes) if not dry_run else 0
        pct_bytes = (diff_bytes / b_bytes * 100) if b_bytes > 0 and not dry_run else 0.0

        status_after = "Dự kiến" if dry_run else "Sau khi gộp"
        mib_after_str = f"{a_mib:>18.2f}" if not dry_run else f"{'~ ' + str(round(b_mib, 2)):>18}"

        print(f"{source_name:<22} | {'Trước khi gộp':<16} | {b_dt:>7} | {b_files:>12} | {b_rec:>14,d} | {b_mib:>18.2f}")
        print(f"{'':<22} | {status_after:<16} | {a_dt:>7} | {a_files:>12} | {a_rec:>14,d} | {mib_after_str}")
        
        file_change_str = f"{diff_files:+d} ({pct_files:.1f}%)"
        byte_change_str = f"{format_bytes(diff_bytes):+0.2f} MiB ({pct_bytes:.1f}%)" if not dry_run else "Chưa ghi file"
        print(f"{'':<22} | {'Chênh lệch':<16} | {'0':>7} | {file_change_str:>12} | {'0':>14} | {byte_change_str:>18}")
        print(sub_separator)

    # Dòng tổng cộng
    total_b_mib = format_bytes(total_bytes_before)
    total_a_mib = format_bytes(total_bytes_after) if not dry_run else total_b_mib
    total_diff_files = total_files_after - total_files_before
    total_pct_files = (total_diff_files / total_files_before * 100) if total_files_before > 0 else 0.0
    total_diff_bytes = (total_bytes_after - total_bytes_before) if not dry_run else 0
    total_pct_bytes = (total_diff_bytes / total_bytes_before * 100) if total_bytes_before > 0 and not dry_run else 0.0

    total_status_after = "Dự kiến" if dry_run else "Sau khi gộp"
    total_mib_after_str = f"{total_a_mib:>18.2f}" if not dry_run else f"{'~ ' + str(round(total_b_mib, 2)):>18}"

    print(f"{'TỔNG CỘNG':<22} | {'Trước khi gộp':<16} | {total_dt_before:>7} | {total_files_before:>12} | {total_records_before:>14,d} | {total_b_mib:>18.2f}")
    print(f"{'':<22} | {total_status_after:<16} | {total_dt_after:>7} | {total_files_after:>12} | {total_records_after:>14,d} | {total_mib_after_str}")
    
    tot_file_change = f"{total_diff_files:+d} ({total_pct_files:.1f}%)"
    tot_byte_change = f"{format_bytes(total_diff_bytes):+0.2f} MiB ({total_pct_bytes:.1f}%)" if not dry_run else "Chưa ghi file"
    print(f"{'':<22} | {'Chênh lệch':<16} | {'0':>7} | {tot_file_change:>12} | {'0':>14} | {tot_byte_change:>18}")
    print(line_separator + "\n")


def main():
    args = parse_args()
    raw_dir = Path(args.raw_dir).resolve()

    if not raw_dir.exists() or not raw_dir.is_dir():
        print(f"[LỖI] Thư mục raw không tồn tại hoặc không hợp lệ: {raw_dir}", file=sys.stderr)
        sys.exit(1)

    if args.max_records_per_file < 0:
        print("[LỖI] --max-records-per-file phải là số không âm (>= 0).", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print(" BẮT ĐẦU QUÁ TRÌNH GỘP FILE RAW (STEAM PLAYER INSIGHTS)")
    print("=" * 70)
    print(f" Thư mục dữ liệu: {raw_dir}")
    print(f" Chế độ: {'DRY-RUN (Chạy thử, không sửa file)' if args.dry_run else 'THỰC THI (Compacting)'}")
    print(f" Sao lưu: {args.backup_dir if args.backup_dir else 'Không yêu cầu'}")
    print(f" Giới hạn dòng/file: {'Không giới hạn (1 file/phân vùng)' if args.max_records_per_file == 0 else args.max_records_per_file}")
    print("-" * 70)

    # 1. Tìm các thư mục nguồn hợp lệ
    all_subdirs = sorted([d for d in raw_dir.iterdir() if d.is_dir()])
    source_dirs = [d for d in all_subdirs if is_source_directory(d)]

    if not source_dirs:
        print(f"[CẢNH BÁO] Không tìm thấy thư mục nguồn hợp lệ nào (có partition dt=) trong {raw_dir}")
        sys.exit(0)

    print(f"[INFO] Phát hiện {len(source_dirs)} nguồn dữ liệu:")
    for s in source_dirs:
        dt_count = len(get_dt_partitions(s))
        print(f"  - {s.name:<25} ({dt_count} phân vùng dt=)")

    # 2. Xử lý sao lưu nếu được chỉ định
    if args.backup_dir:
        backup_dir = Path(args.backup_dir).resolve()
        if backup_dir == raw_dir:
            print("[LỖI] Thư mục backup không được trùng với thư mục raw!", file=sys.stderr)
            sys.exit(1)
        if not args.dry_run:
            backup_raw_data(raw_dir, backup_dir)
        else:
            print(f"\n[DRY-RUN] Dự kiến sao lưu từ '{raw_dir}' sang '{backup_dir}' (bỏ qua do đang dry-run).")

    # 3. Tiến hành gộp dữ liệu từng nguồn
    summary_data = {}
    start_time = time.time()

    try:
        for s_dir in source_dirs:
            print(f"\n>>> Đang xử lý nguồn: {s_dir.name}")
            dt_partitions = get_dt_partitions(s_dir)

            s_before_files = 0
            s_before_rec = 0
            s_before_bytes = 0
            s_after_files = 0
            s_after_bytes = 0

            for dt_p in dt_partitions:
                b_f, b_r, b_b, a_f, a_b = compact_partition(
                    dt_p,
                    max_records_per_file=args.max_records_per_file,
                    dry_run=args.dry_run,
                )
                s_before_files += b_f
                s_before_rec += b_r
                s_before_bytes += b_b
                s_after_files += a_f
                s_after_bytes += a_b

            summary_data[s_dir.name] = {
                "before_dt": len(dt_partitions),
                "after_dt": len(dt_partitions),
                "before_files": s_before_files,
                "after_files": s_after_files,
                "before_records": s_before_rec,
                "after_records": s_before_rec,
                "before_bytes": s_before_bytes,
                "after_bytes": s_after_bytes,
            }

        elapsed_time = time.time() - start_time

        # 4. In bảng thống kê tổng hợp
        print_summary_table(summary_data, dry_run=args.dry_run)
        print(f"Thời gian thực thi: {elapsed_time:.2f} giây.")
        print("=" * 70)

    except Exception as fatal_err:
        print(f"\n[LỖI NGHIÊM TRỌNG] Quá trình gộp thất bại: {fatal_err}", file=sys.stderr)
        print("[BẢO TOÀN DỮ LIỆU] Dữ liệu gốc được bảo toàn 100%. Các file tạm đã được dọn dẹp an toàn.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
