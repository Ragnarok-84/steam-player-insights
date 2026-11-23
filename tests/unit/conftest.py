import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """SparkSession local nhỏ, đọc/ghi file local. Chạy trên Linux/WSL (Windows cần hadoop.dll thật)."""
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["HDFS_NAMENODE_URL"] = ""
    os.environ["HIVE_METASTORE_URI"] = ""  # không để .env của máy dev kéo test vào Hive
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"
    from processing.common.spark_utils import get_spark_session
    s = get_spark_session("unit-tests", master="local[2]")
    yield s
    s.stop()
