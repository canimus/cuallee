# pip install great-expectations==0.18.13

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()

df = spark.read.parquet("/data/*.parquet")
check = SparkDFDataset(df)


start = datetime.now()

check_unique = [
    check.expect_column_values_to_be_unique(name).success for name in df.columns
]
check_complete = [
    check.expect_column_values_to_not_be_null(name).success for name in df.columns
]

end = datetime.now()
print(check_unique + check_complete)
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: great-expecatations")
spark.stop()
