from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, DateType, \
    TimestampType
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
    .config("spark.python.worker.reuse", "false")  # tránh lỗi worker treo
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

schema = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("QUARTER", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY_OF_MONTH", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("FL_DATE", TimestampType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("OP_CARRIER_AIRLINE_ID", IntegerType(), True),
    StructField("OP_CARRIER", StringType(), True),
    StructField("TAIL_NUM", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    StructField("ORIGIN_AIRPORT_ID", IntegerType(), True),
    StructField("ORIGIN_AIRPORT_SEQ_ID", IntegerType(), True),
    StructField("ORIGIN_CITY_MARKET_ID", IntegerType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY_NAME", StringType(), True),
    StructField("ORIGIN_STATE_ABR", StringType(), True),
    StructField("ORIGIN_STATE_FIPS", IntegerType(), True),
    StructField("ORIGIN_STATE_NM", StringType(), True),
    StructField("ORIGIN_WAC", IntegerType(), True),
    StructField("DEST_AIRPORT_ID", IntegerType(), True),
    StructField("DEST_AIRPORT_SEQ_ID", IntegerType(), True),
    StructField("DEST_CITY_MARKET_ID", IntegerType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY_NAME", StringType(), True),
    StructField("DEST_STATE_ABR", StringType(), True),
    StructField("DEST_STATE_FIPS", IntegerType(), True),
    StructField("DEST_STATE_NM", StringType(), True),
    StructField("DEST_WAC", IntegerType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", IntegerType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("DEP_DELAY_NEW", DoubleType(), True),
    StructField("DEP_DEL15", DoubleType(), True),
    StructField("DEP_DELAY_GROUP", IntegerType(), True),
    StructField("DEP_TIME_BLK", StringType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", IntegerType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("ARR_DELAY_NEW", DoubleType(), True),
    StructField("ARR_DEL15", DoubleType(), True),
    StructField("ARR_DELAY_GROUP", IntegerType(), True),
    StructField("ARR_TIME_BLK", StringType(), True),
    StructField("CANCELLED", DoubleType(), True),
    StructField("DIVERTED", DoubleType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("FLIGHTS", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("DISTANCE_GROUP", IntegerType(), True),
    StructField("DIV_AIRPORT_LANDINGS", IntegerType(), True)
])
