# cuallee
Meaning `good` in Aztec (Nahuatl), _pronounced: QUAL-E_

This library provides an intuitive `API` to describe `checks` initially just for `PySpark` dataframes `v3.3.0`. And extended to `pandas`, `snowpark`, `duckdb`, and more.
It is a replacement written in pure `python` of the `pydeequ` framework.

I gave up in _deequ_ as after extensive use, the API is not user-friendly, the Python Callback servers produce additional costs in our compute clusters, and the lack of support to the newest version of PySpark.

As result `cuallee` was born

This implementation goes in hand with the latest API from PySpark and uses the `Observation` API to collect metrics
at the lower cost of computation. 
When benchmarking against pydeequ, `cuallee` uses circa <3k java classes underneath and **remarkably** less memory.

## Support

`cuallee` is the data quality framework truly dataframe agnostic.

Provider | API | Versions
 ------- | ----------- | ------
![snowflake](logos/snowflake.svg?raw=true "Snowpark DataFrame API")| `snowpark` | `1.4.0`
![databricks](logos/databricks.svg?raw=true "PySpark DataFrame API")| `pyspark` | `3.4.0`, `3.3.x`, `3.2.x`
![pandas](logos/pandas.svg?raw=true "Pandas DataFrame API")| `pandas`| `2.0.1`, `1.5.x`, `1.4.x`
![duckdb](logos/duckdb.png?raw=true "DuckDB API")|`duckdb` | `0.7.1`
![polars](logos/polars.svg?raw=true "Polars API")|`polars`|`0.15.x (wip)` 
 
 <sub>Logos are trademarks of their own brands.</sub>

## Stats
[![PyPI version](https://badge.fury.io/py/cuallee.svg)](https://badge.fury.io/py/cuallee)
[![ci](https://github.com/canimus/cuallee/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/canimus/cuallee/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/canimus/cuallee/branch/main/graph/badge.svg?token=D7SOV620MS)](https://codecov.io/gh/canimus/cuallee)
[![](https://img.shields.io/pypi/dm/cuallee.svg?style=popout-square)](https://pypi.org/project/cuallee/)
[![License](https://img.shields.io/github/license/canimus/cuallee.svg?style=popout-square)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
## Install
```bash
pip install cuallee
```

## Checks

The most common checks for data integrity validations are `completeness` and `uniqueness` an example of this dimensions shown below:

```python
from cuallee import Check, CheckLevel # WARN:0, ERR: 1

# Nulls on column Id
check = Check(CheckLevel.WARNING, "Completeness")
(
    check
    .is_complete("id")
    .is_unique("id")
    .validate(df)
).show() # Returns a pyspark.sql.DataFrame
```

### Dates

Perhaps one of the most useful features of `cuallee` is its extensive number of checks for `Date` and `Timestamp` values. Including, validation of ranges, set operations like inclusion, or even a verification that confirms `continuity on dates` using the `is_daily` check function.

```python
# Unique values on id
check = Check(CheckLevel.WARNING, "CheckIsBetweenDates")
df = spark.sql(
    """
    SELECT 
        explode(
            sequence(
                to_date('2022-01-01'), 
                to_date('2022-01-10'), 
                interval 1 day)) as date
    """)
assert (
    check.is_between("date", "2022-01-01", "2022-01-10")
    .validate(df)
    .first()
    .status == "PASS"
)
```

### Membership

Other common test is the validation of `list of values` as part of the multiple integrity checks required for better quality data.

```python
df = spark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["ID", "value"])
check = Check(CheckLevel.WARNING, "is_contained_in_number_test")
check.is_contained_in("value", (10, 15, 20, 25)).validate(df)
```

### Regular Expressions

When it comes to the flexibility of matching, regular expressions are always to the rescue. `cuallee` makes use of the regular expressions to validate that fields of type `String` conform to specific patterns.

```python
df = spark.createDataFrame([[1, "is_blue"], [2, "has_hat"], [3, "is_smart"]], ["ID", "desc"])
check = Check(CheckLevel.WARNING, "has_pattern_test")
check.has_pattern("desc", r"^is.*t$") # only match is_smart 33% of rows.
check.validate(df).first().status == "FAIL"
```

### Anomalies

Statistical tests are a great aid for verifying anomalies on data. Here an example that shows that will `PASS` only when `40%` of data is inside the interquartile range

```python
df = spark.range(10)
check = Check(CheckLevel.WARNING, "IQR_Test")
check.is_inside_interquartile_range("id", pct=0.4)
check.validate(df).first().status == "PASS"

+---+-------------------+-----+-------+------+-----------------------------+-----+----+----------+---------+--------------+------+
|id |timestamp          |check|level  |column|rule                         |value|rows|violations|pass_rate|pass_threshold|status|
+---+-------------------+-----+-------+------+-----------------------------+-----+----+----------+---------+--------------+------+
|1  |2022-10-19 00:09:39|IQR  |WARNING|id    |is_inside_interquartile_range|10000|10  |4         |0.6      |0.4           |PASS  |
+---+-------------------+-----+-------+------+-----------------------------+-----+----+----------+---------+--------------+------+
```

### Workflows (Process Mining)
Besides the common `citizen-like` checks, `cuallee` offers out-of-the-box real-life checks. For example, suppose that you are working __SalesForce__ or __SAP__ environment. Very likely your business processes will be driven by a lifecycle:
- `Order-To-Cash` 
- `Request-To-Pay` 
- `Inventory-Logistics-Delivery` 
- Others.
 In this scenario, `cuallee` offers the ability that the sequence of events registered over time, are according to a sequence of events, like the example below:

 ```python
import pyspark.sql.functions as F
from cuallee import Check, CheckLevel

data = pd.DataFrame({
    "name":["herminio", "herminio", "virginie", "virginie"], 
    "event":["new","active", "new", "active"], 
    "date": ["2022-01-01", "2022-01-02", "2022-01-03", "2022-02-04"]}
    )
df = spark.createDataFrame(data).withColumn("date", F.to_date("date"))

# Cuallee Process Mining
# Testing that all edges on workflows
check = Check(CheckLevel.WARNING, "WorkflowViolations")

# Validate that 50% of data goes from new => active
check.has_workflow("name", "event", "date", [("new", "active")], pct=0.5)
check.validate(df).show(truncate=False)

+---+-------------------+------------------+-------+-------------------------+------------+--------------------+----+----------+---------+--------------+------+
|id |timestamp          |check             |level  |column                   |rule        |value               |rows|violations|pass_rate|pass_threshold|status|
+---+-------------------+------------------+-------+-------------------------+------------+--------------------+----+----------+---------+--------------+------+
|1  |2022-11-07 23:08:50|WorkflowViolations|WARNING|('name', 'event', 'date')|has_workflow|(('new', 'active'),)|4   |2.0       |0.5      |0.5           |PASS  |
+---+-------------------+------------------+-------+-------------------------+------------+--------------------+----+----------+---------+--------------+------+

 ```

### `cuallee` __VS__ `pydeequ`
In the `test` folder there are `docker` containers with the requirements to match the tests. Also a `perftest.py` available at the root folder for interests.

```
# 1000 rules / # of seconds

cuallee: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 162.00
pydeequ: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 322.00
```


## Catalogue

Check | Description | DataType
 ------- | ----------- | ----
`is_complete` | Zero `nulls` | _agnostic_
`is_unique` | Zero `duplicates` | _agnostic_
`are_complete` | Zero `nulls` on group of columns | _agnostic_
`are_unique` | Composite primary key check | _agnostic_
`is_greater_than` | `col > x` | _numeric_
`is_positive` | `col > 0` | _numeric_
`is_negative` | `col < 0` | _numeric_
`is_greater_or_equal_than` | `col >= x` | _numeric_
`is_less_than` | `col < x` | _numeric_
`is_less_or_equal_than` | `col <= x` | _numeric_
`is_equal_than` | `col == x` | _numeric_
`is_contained_in` | `col in [a, b, c, ...]` | _agnostic_
`is_in` | Alias of `is_contained_in` | _agnostic_
`is_between` | `a <= col <= b` | _numeric, date_
`has_pattern` | Matching a pattern defined as a `regex` | _string_
`has_min` | `min(col) == x` | _numeric_
`has_max` | `max(col) == x` | _numeric_
`has_std` | `σ(col) == x` | _numeric_
`has_mean` | `μ(col) == x` | _numeric_
`has_sum` | `Σ(col) == x` | _numeric_
`has_percentile` | `%(col) == x` | _numeric_
`has_max_by` | A utilitary predicate for `max(col_a) == x for max(col_b)`  | _agnostic_
`has_min_by` | A utilitary predicate for `min(col_a) == x for min(col_b)`  | _agnostic_
`has_correlation` | Finds correlation between `0..1` on `corr(col_a, col_b)` | _numeric_
`has_entropy` | Calculates the entropy of a column `entropy(col) == x` for classification problems | _numeric_
`is_inside_interquartile_range` | Verifies column values reside inside limits of interquartile range `Q1 <= col <= Q3` used on anomalies.  | _numeric_
`is_in_millions` | `col >= 1e6` | _numeric_
`is_in_billions` | `col >= 1e9` | _numeric_
`is_on_weekday` | For date fields confirms day is between `Mon-Fri` | _date_
`is_on_weekend` | For date fields confirms day is between `Sat-Sun` | _date_
`is_on_monday` | For date fields confirms day is `Mon` | _date_
`is_on_tuesday` | For date fields confirms day is `Tue` | _date_
`is_on_wednesday` | For date fields confirms day is `Wed` | _date_
`is_on_thursday` | For date fields confirms day is `Thu` | _date_
`is_on_friday` | For date fields confirms day is `Fri` | _date_
`is_on_saturday` | For date fields confirms day is `Sat` | _date_
`is_on_sunday` | For date fields confirms day is `Sun` | _date_
`is_on_schedule` | For date fields confirms time windows i.e. `9:00 - 17:00` | _timestamp_
`is_daily` | Can verify daily continuity on date fields by default. `[2,3,4,5,6]` which represents `Mon-Fri` in PySpark. However new schedules can be used for custom date continuity | _date_
`has_workflow` | Adjacency matrix validation on `3-column` graph, based on `group`, `event`, `order` columns.  | _agnostic_
`satisfies` | An open `SQL expression` builder to construct custom checks | _agnostic_
`validate` | The ultimate transformation of a check with a `dataframe` input for validation | _agnostic_



## Snowflake Connection
In order to establish a connection to your SnowFlake account `cuallee` relies in the following environment variables to be avaialble in your environment:
- `SF_ACCOUNT`
- `SF_USER`
- `SF_PASSWORD`
- `SF_ROLE`
- `SF_WAREHOUSE`
- `SF_DATABASE`
- `SF_SCHEMA`

## Databricks Connection
By default `cuallee` will search for a SparkSession available in the `globals` so there is literally no need to ~~`SparkSession.builder`~~. When working in a local environment it will automatically search for an available session, or start one.

## DuckDB

For testing on `duckdb` simply pass your table name to your check _et voilà_

```python
import duckdb
conn = duckdb.connect(":memory:")
check = Check(CheckLevel.WARNING, "DuckDB", table_name="temp/taxi/*.parquet")
check.is_complete("VendorID")
check.is_complete("tpep_pickup_datetime")
check.validate(conn)

   id            timestamp check    level                column         rule value      rows  violations  pass_rate  pass_threshold status
0   1  2022-10-31 23:15:06  test  WARNING              VendorID  is_complete   N/A  19817583         0.0        1.0             1.0   PASS
1   2  2022-10-31 23:15:06  test  WARNING  tpep_pickup_datetime  is_complete   N/A  19817583         0.0        1.0             1.0   PASS
```

## Roadmap

`100%` data frame agnostic implementation of data quality checks.
Define once, `run everywhere`
- [x] PySpark 3.3.0
- [x] PySpark 3.2.x 
- [x] Snowpark DataFrame
- [x] Pandas DataFrame
- [x] DuckDB Tables
- Polars DataFrame
- SQLite Tables
- MS-SQL Tables

Whilst expanding the functionality feels a bit as an overkill because you most likely can connect `spark` via its drivers to whatever `DBMS` of your choice.
In the desire to make it even more `user-friendly` we are aiming to make `cuallee` portable to all the providers above.

## Authors
- [canimus](https://github.com/canimus) / Herminio Vazquez / 🇲🇽 
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot / 🇫🇷 



## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Utrecht 🇳🇱