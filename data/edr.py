from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, DateType, TimestampType
from pyspark.sql.functions import upper, pandas_udf, expr
import pyspark.pandas as ps

from datetime import datetime, date
import pandas as pd
import numpy as np
import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

spark = (
    SparkSession.builder
    .appName("PandasToSparkTest")
    .master("local[*]")  # dùng toàn bộ CPU
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.python.worker.reuse", "false")   # tránh lỗi worker treo
    .config("spark.network.timeout", "300s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)
psdf = ps.read_csv("archive/T_ONTIME_REPORTING_2021_M2.csv")

psdf.describe()

print(psdf.dtypes)