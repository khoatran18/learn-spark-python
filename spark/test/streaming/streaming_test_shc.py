# streaming_test_shc.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# spark-submit --jars core-spark-shc-1.1.1-2.1-s_2.13.jar -packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 streaming_test_shc.py

# =========================
# Spark session
# =========================
spark = (
    SparkSession.builder
    .appName("KafkaToHBaseSHC")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.network.timeout", "300s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

# =========================
# Kafka stream
# =========================
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("includeHeaders", "true")
    .load()
)

df_kafka = df_raw.selectExpr("CAST(key AS STRING) AS key",
                             "CAST(value AS STRING) AS value")

# =========================
# Transformation example
# =========================
df_processed = df_kafka.withColumn("value_upper", col("value").upper())

# =========================
# HBase catalog for SHC
# =========================
hbase_catalog = """
{
    "table":{"namespace":"default", "name":"test"},
    "rowkey":"key",
    "columns":{
        "key":{"cf":"rowkey", "col":"key", "type":"string"},
        "value_upper":{"cf":"cf1", "col":"value", "type":"string"}
    }
}
"""

# =========================
# Write to HBase via foreachBatch
# =========================
def write_to_hbase(batch_df, batch_id):
    batch_df.write \
        .options(catalog=hbase_catalog, newtable="5") \
        .format("org.apache.hadoop.hbase.spark") \
        .save()

query = (
    df_processed.writeStream
    .foreachBatch(write_to_hbase)
    .outputMode("update")  # hoặc "append"
    .start()
)

query.awaitTermination()
