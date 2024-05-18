# Usage

Validating data sets is about creating a `Check` and adding rules into it.
You can choose from different types: `numeric`, `date algebra`, `range of values`, `temporal`, and many others.

A `Check` provides a declarative interface to build a comprehensive validation on a dataframe as shown below:

## Libraries

```python
# Imports
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

# Cuallee v0.10.3
from cuallee import Check, CheckLevel
from cuallee.pyspark_validation import _field_type_filter
```

## Session

```python

# Spark Session
spark = SparkSession.builder.getOrCreate()

```

## Check

```python
# Check 
check = Check(CheckLevel.WARNING, "TaxiNYCheck")

```


## Data
```python
# !wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
# !wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
df = spark.read.parquet("*yellow*tripdata*.parquet")
df = df.withColumn("date", F.to_date("tpep_pickup_datetime"))
```


## Checks
```python
# Confirm that all records are not null for all columns in dataframe
[check.is_complete(name) for name in df.columns]

# Verify taxi ride distance is positive
[check.is_greater_than(name, 0) for name in _field_type_filter(df, T.NumericType)] 

# Confirm that tips are not outliers
[check.is_less_than(name, 1e4) for name in _field_type_filter(df, T.NumericType)] 

# 70% of data is on weekdays
check.is_on_weekday("date", .7)

# Binary classification fields
[check.has_entropy(name, 1.0, 0.5) for name in _field_type_filter(df, T.NumericType)] 

# Percentage of big tips
[check.is_between(name, (1000,2000)) for name in _field_type_filter(df, T.NumericType)]

# Confirm 22 years of data
check.is_between("date", ("2000-01-01", "2022-12-31"))
```


## Validation
```python
check.validate(df).show()

```

If you want to display full results use:

`check.validate(df).show(n=check.sum, truncate=False)`


## Code

??? example "Check"

   

    ``` python
    # Imports
    from pyspark.sql import SparkSession
    import pyspark.sql.types as T
    import pyspark.sql.functions as F

    # Cuallee
    from cuallee import Check, CheckLevel
    from cuallee.pyspark_validation import _field_type_filter

    # Session
    spark = SparkSession.builder.getOrCreate()

    # Check
    check = Check(CheckLevel.WARNING, "TaxiNYCheck")

    # !wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    # !wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
    df = spark.read.parquet("*yellow*tripdata*.parquet")
    df = df.withColumn("date", F.to_date("tpep_pickup_datetime"))

    # Confirm that all records are not null for all columns in dataframe
    [check.is_complete(name) for name in df.columns]

    # Verify taxi ride distance is positive
    [check.is_greater_than(name, 0) for name in _field_type_filter(df, T.NumericType)] 

    # Confirm that tips are not outliers
    [check.is_less_than(name, 1e4) for name in _field_type_filter(df, T.NumericType)] 

    # 70% of data is on weekdays
    check.is_on_weekday("date", .7)

    # Binary classification fields
    [check.has_entropy(name, 1.0, 0.5) for name in _field_type_filter(df, T.NumericType)] 

    # Percentage of big tips
    [check.is_between(name, (1000,2000)) for name in _field_type_filter(df, T.NumericType)]

    # Confirm 22 years of data
    check.is_between("date", ("2000-01-01", "2022-12-31"))

    # Validate
    check.validate(df).show()
    ```

    __Result:__

    ``` markdown
    # Result
    +---+-------------------+-----------+-------+--------------------+-----------+-----+-------+----------+------------------+--------------+------+
    | id|          timestamp|      check|  level|              column|       rule|value|   rows|violations|         pass_rate|pass_threshold|status|
    +---+-------------------+-----------+-------+--------------------+-----------+-----+-------+----------+------------------+--------------+------+
    |  1|2024-05-10 21:00:09|TaxiNYCheck|WARNING|            VendorID|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    |  2|2024-05-10 21:00:09|TaxiNYCheck|WARNING|tpep_pickup_datetime|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    |  3|2024-05-10 21:00:09|TaxiNYCheck|WARNING|tpep_dropoff_date...|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    |  4|2024-05-10 21:00:09|TaxiNYCheck|WARNING|     passenger_count|is_complete|  N/A|5972150|    325772|0.9454514705759233|           1.0|  FAIL|
    |  5|2024-05-10 21:00:09|TaxiNYCheck|WARNING|       trip_distance|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    |  6|2024-05-10 21:00:09|TaxiNYCheck|WARNING|          RatecodeID|is_complete|  N/A|5972150|    325772|0.9454514705759233|           1.0|  FAIL|
    |  7|2024-05-10 21:00:09|TaxiNYCheck|WARNING|  store_and_fwd_flag|is_complete|  N/A|5972150|    325772|0.9454514705759233|           1.0|  FAIL|
    |  8|2024-05-10 21:00:09|TaxiNYCheck|WARNING|        PULocationID|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    |  9|2024-05-10 21:00:09|TaxiNYCheck|WARNING|        DOLocationID|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 10|2024-05-10 21:00:09|TaxiNYCheck|WARNING|        payment_type|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 11|2024-05-10 21:00:09|TaxiNYCheck|WARNING|         fare_amount|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 12|2024-05-10 21:00:09|TaxiNYCheck|WARNING|               extra|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 13|2024-05-10 21:00:09|TaxiNYCheck|WARNING|             mta_tax|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 14|2024-05-10 21:00:09|TaxiNYCheck|WARNING|          tip_amount|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 15|2024-05-10 21:00:09|TaxiNYCheck|WARNING|        tolls_amount|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 16|2024-05-10 21:00:09|TaxiNYCheck|WARNING|improvement_surch...|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 17|2024-05-10 21:00:09|TaxiNYCheck|WARNING|        total_amount|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    | 18|2024-05-10 21:00:09|TaxiNYCheck|WARNING|congestion_surcharge|is_complete|  N/A|5972150|    325772|0.9454514705759233|           1.0|  FAIL|
    | 19|2024-05-10 21:00:09|TaxiNYCheck|WARNING|         Airport_fee|is_complete|  N/A|5972150|    325772|0.9454514705759233|           1.0|  FAIL|
    | 20|2024-05-10 21:00:09|TaxiNYCheck|WARNING|                date|is_complete|  N/A|5972150|         0|               1.0|           1.0|  PASS|
    +---+-------------------+-----------+-------+--------------------+-----------+-----+-------+----------+------------------+--------------+------+
    ```