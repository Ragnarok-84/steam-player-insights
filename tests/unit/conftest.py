import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """SparkSession local nhỏ; chỉ tính toán trong bộ nhớ (không ghi file, chạy được cả trên Windows)."""
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["HDFS_NAMENODE_URL"] = ""
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"
    from processing.common.spark_utils import get_spark_session
    s = get_spark_session("unit-tests", master="local[2]")
    yield s
    s.stop()
