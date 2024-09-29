from datetime import datetime

import pyspark.sql.functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config("spark.jars", "/libs/deequ-2.0.7-spark-3.5.jar")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

check = Check(spark, CheckLevel.Warning, "PydeequBenchmark")
df = spark.read.parquet("/data/*.parquet")

[check.isComplete(name) for name in df.columns]
[check.isUnique(name) for name in df.columns]


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
