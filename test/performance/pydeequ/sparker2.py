from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee.pyspark_validation import numeric_fields, timestamp_fields
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from datetime import datetime

spark = (
    SparkSession.builder.config("spark.jars", "deequ-2.0.1-spark-3.2.jar")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

check = Check(spark, CheckLevel.Warning, "PydeequBenchmark")
df = spark.read.parquet("data/*.parquet")

[check.isComplete(name) for name in df.columns]
[check.isGreaterThan(name, "total_amount") for name in numeric_fields(df)]
[check.isLessThan(name, "total_amount") for name in numeric_fields(df)]
# [check.hasEntropy(name, 1.0, 0.5) for name in numeric_fields(df)]
[
    check.satisfies(f"{name} BETWEEN 1000 AND 2000", "BETWEEN 1k-2k", lambda x: x == 1)
    for name in numeric_fields(df)
]
[
    check.satisfies(
        f"{name} BETWEEN '2000-01-01' AND '2022-12-31'",
        "BETWEEN Jan-Dec",
        lambda x: x >= 0.9,
    )
    for name in timestamp_fields(df)
]


start = datetime.now()
VerificationResult.checkResultsAsDataFrame(
    spark, VerificationSuite(spark).onData(df).addCheck(check).run()
).show(n=1000, truncate=False)
end = datetime.now()
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: pydeequ")
spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()
