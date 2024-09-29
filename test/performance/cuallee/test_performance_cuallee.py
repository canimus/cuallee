from datetime import datetime
from os import truncate

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from cuallee import Check, CheckLevel

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()

check = Check(CheckLevel.WARNING, "CualleeBenchmark")
df = spark.read.parquet("/data/*.parquet")


[check.is_complete(name) for name in df.columns]
[check.is_unique(name, approximate=True) for name in df.columns]


start = datetime.now()
check.validate(df).show(n=int(len(df.columns) * 2), truncate=False)
end = datetime.now()
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: cuallee")
spark.stop()
