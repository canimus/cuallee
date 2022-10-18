# cuallee
Meaning `good` in Aztec (Nahuatl), _pronounced: QUAL-E_


This library provides an intuitive `API` to describe `checks` for Apache PySpark DataFrames `v3.3.0`.
It is a replacement written in pure `python` of the `pydeequ` framework.

I gave up in _deequ_ as after extensive use, the API is not user-friendly, the Python Callback servers produce additional costs in our compute clusters, and the lack of support to the newest version of PySpark.

As result `cuallee` was born, with a simplistic, extremely user friendly interface, checks and rules that will solve the common-sense data quality checks, avoiding the non-sense.


This implementation goes in hand with the latest API from PySpark and uses the `Observation` API to collect metrics
at the lower cost of computation. 
When benchmarking against pydeequ, `cuallee` uses circa <3k java classes underneath and **remarkably** less memory.
 

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


## Catalogue

Check | Description | DataType
 ------- | ----------- | ----
`is_complete` | Zero `nulls` | _agnostic_
`is_unique` | Zero `duplicates` | _agnostic_
`are_complete` | Zero `nulls` on group of columns | _agnostic_
`are_unique` | Composite primary key check | _agnostic_
`is_greater_than` | `col > x` | _numeric_
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
`has_percentile` | `%(col) == x` | _numeric_
`has_max_by` | A utilitary predicate for `max(col_a) == x for max(col_b)`  | _agnostic_
`has_min_by` | A utilitary predicate for `min(col_a) == x for min(col_b)`  | _agnostic_
`has_correlation` | Finds correlation between `0..1` on `corr(col_a, col_b)` | _numeric_
`has_entropy` | Calculates the entropy of a column `entropy(col) == x` for classification problems | _numeric_
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
`satisfies` | An open `SQL expression` builder to construct custom checks | _agnostic_
`validate` | The ultimate transformation of a check with a `dataframe` input for validation | _agnostic_
`samples` | Returns per rule. A sample of the data with the `predicates` failing the check | _agnostic_



## Roadmap

`100%` data frame agnostic implementation of data quality checks.
Define once, `run everywhere`
- Snowpark DataFrame
- Pandas DataFrame
- Polars DataFrame
- SQlite3 Tables
- DuckDB Tables
- SQL Tables


## Authors
- [canimus](https://github.com/canimus) / Herminio Vazquez
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot


## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.