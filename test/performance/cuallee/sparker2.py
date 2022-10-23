from os import truncate
from cuallee import Check, CheckLevel
from cuallee.pyspark_validation import numeric_fields, timestamp_fields
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()

check = Check(CheckLevel.WARNING, "CualleeBenchmark")
df = spark.read.parquet("data/*.parquet")

[check.is_complete(name) for name in df.columns]
[check.is_greater_than(name, F.col("total_amount")) for name in numeric_fields(df)]
[check.is_less_than(name, F.col("total_amount")) for name in numeric_fields(df)]
# [check.has_entropy(name, 1.0, 0.5) for name in numeric_fields(df)]
[check.is_between(name, (1000, 2000)) for name in numeric_fields(df)]
[
    check.is_between(name, ("2000-01-01", "2022-12-31"), 0.9)
    for name in timestamp_fields(df)
]

start = datetime.now()
check.validate(df).show(n=1000, truncate=False)
end = datetime.now()
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: cuallee")
spark.stop()
