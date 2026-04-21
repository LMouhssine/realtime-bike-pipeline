from __future__ import annotations

import sys
from pathlib import Path
import os

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark_session():
    if sys.platform == "win32" and sys.version_info >= (3, 13):
        pytest.skip(
            "Local PySpark worker tests are skipped on Windows with Python 3.13. "
            "Run them with Python 3.11/3.12 or inside the Spark container."
        )
    pyspark = pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("citybike-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()
