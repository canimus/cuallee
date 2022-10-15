from os import truncate
from cuallee import Check, CheckLevel
from cuallee.spark_validation import numeric_fields, timestamp_fields
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def init():

    spark = SparkSession.builder.config("spark.driver.memory", "50g").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    quiet_logs(spark.sparkContext)
    
    df = spark.read.parquet("temp/taxi/*.parquet")

    check = Check(CheckLevel.WARNING, "Taxi")
    # [check.is_complete(name) for name in df.columns]
    # [check.is_greater_than(name, 0) for name in numeric_fields(df)]
    # [check.is_less_than(name, 1e4) for name in numeric_fields(df)]
    # [check.is_on_weekday(name, .7) for name in timestamp_fields(df)]
    # # [check.has_entropy(name, 1.0, 0.5) for name in numeric_fields(df)]
    # [check.is_between(name, (1000,2000)) for name in numeric_fields(df)]
    # [check.is_between(name, ("2000-01-01", "2022-12-31")) for name in timestamp_fields(df)]
    for i in range(1000):
        check.is_greater_than("fare_amount", i)

    return spark, df, check

def with_validate(df, rule):
    return rule.validate(df)

def with_select(df, c):
    df.select([(F.round(x[1]/F.count("*"),2)).alias(f"{x[0].method}({x[0].column})") for x in c._compute.values()])

if __name__ == "__main__":
    
    spark, df, rule = init()
    start = datetime.now()
    r = with_validate(df, rule)
    # r = with_select(df, rule)
    r.show(n=1000, truncate=False)
    end = datetime.now()
    print("START:",start)
    print("END:",end)
    print("ELAPSED:", end-start)
    print("RULES:", len(rule.rules))
    print("FRAMEWORK: cuallee [0.0.14]")
# 0.10513386

