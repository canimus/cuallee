from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from datetime import datetime

spark = SparkSession.builder.config("spark.jars","deequ-2.0.1-spark-3.2.jar").config("spark.driver.memory", "16g").getOrCreate()

check = Check(spark, CheckLevel.Warning, "PydeequBenchmark")
df = spark.read.parquet("data/*.parquet")

for i in range(1000):
   check.satisfies(f"fare_amount > {i}", "GreaterThan", lambda x: x == 1.0)


start = datetime.now()
VerificationResult.checkResultsAsDataFrame(spark, VerificationSuite(spark).onData(df).addCheck(check).run()).show(n=1000, truncate=False)
end = datetime.now()
elapsed=end-start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: pydeequ")
spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()

