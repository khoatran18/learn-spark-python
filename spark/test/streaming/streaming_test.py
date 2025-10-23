from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, DateType, TimestampType
from pyspark.sql.functions import upper, pandas_udf, expr
import pyspark.pandas as ps

from datetime import datetime, date
import pandas as pd
import numpy as np
import os

# cd spark/test/streaming/
# spark-submit --master local[*] --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 --repositories http://repo.hortonworks.com/content/groups/public/ --files /etc/hbase/conf/hbase-site.xml streaming_test_shc.py

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

df_raw_kafka = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("includeHeaders", "true")
      .load())
df_raw_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
