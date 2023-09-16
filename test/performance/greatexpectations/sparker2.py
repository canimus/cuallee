from great_expectations.dataset import SparkDFDataset
from great_expectations.core.expectation_configuration import ExpecationConfiguraation
from cuallee.pyspark_validation import numeric_fields, timestamp_fields
from datetime import datetime

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()

df = spark.read.parquet("data/*.parquet")
check = SparkDFDataset(df)

[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": name},
        )
    )
    for name in df.columns
]
[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": name},
        )
    )
    for name in df.columns
]
[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": name, "min_value": 0, "max_value": 100},
        )
    )
    for name in numeric_fields(df)
]
[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": name, "min_value": 5, "max_value": 10},
        )
    )
    for name in numeric_fields(df)
]
[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": name, "min_value": 1000, "max_value": 2000},
        )
    )
    for name in numeric_fields(df)
]

[
    check.append_expectation(
        ExpecationConfiguraation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": name,
                "min_value": "2000-01-01",
                "max_value": "2022-12-31",
                "parse_string_as_datetimes": True,
            },
        )
    )
    for name in timestamp_fields(df)
]

[check.is_greater_than(name, F.col("total_amount")) for name in numeric_fields(df)]
[check.is_less_than(name, F.col("total_amount")) for name in numeric_fields(df)]

start = datetime.now()
result = check.validate()
end = datetime.now()
elapsed = end - start
print("START:", start)
print("END:", end)
print("ELAPSED:", elapsed)
print("FRAMEWORK: great-expecatations")
spark.stop()
