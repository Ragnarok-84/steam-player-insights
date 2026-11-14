import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def ensure_windows_hadoop():
    if os.name != "nt":
        return
    hadoop_dir = ROOT / ".hadoop"
    winutils_exe = hadoop_dir / "bin" / "winutils.exe"
    if not winutils_exe.exists():
        csc = Path(r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe")
        if csc.exists():
            (hadoop_dir / "bin").mkdir(parents=True, exist_ok=True)
            cs_file = hadoop_dir / "bin" / "winutils.cs"
            cs_file.write_text("class P{static int Main(string[] a){return 0;}}", encoding="utf-8")
            import subprocess
            subprocess.run([str(csc), "/nologo", f"/out:{winutils_exe}", "/target:exe", str(cs_file)], check=False)
            if cs_file.exists():
                cs_file.unlink()
    if winutils_exe.exists() and not os.getenv("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = str(hadoop_dir)


@pytest.fixture(scope="session")
def spark():
    """SparkSession local nhỏ; tính toán trong bộ nhớ và ghi Parquet local được cả trên Windows."""
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["HDFS_NAMENODE_URL"] = ""
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"
    ensure_windows_hadoop()
    from processing.common.spark_utils import get_spark_session
    s = get_spark_session("unit-tests", master="local[2]")
    yield s
    s.stop()
