from pyspark.sql import SparkSession, Row
from datetime import datetime, date

print("Start")

spark = SparkSession.builder.getOrCreate()

print("Created successfully")

df = spark.createDataFrame([
    Row(a=1, b=2., c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c="string2", d=date(2000, 1, 2), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=3, b=4., c="string3", d=date(2000, 1, 3), e=datetime(2000, 3, 1, 12, 0)),
])

print(df)