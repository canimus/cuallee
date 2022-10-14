from pyspark.sql import SparkSession
from cuallee import Check, CheckLevel
from datetime import datetime

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()

check = Check(CheckLevel.WARNING, "CualleeBenchmark")
df = spark.read.parquet("data/*.parquet")

for i in range(1000):
    check.is_greater_than("fare_amount", i)

start = datetime.now()
check.validate(df).show(n=1000, truncate=False)
end = datetime.now()
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: cuallee")
spark.stop()
