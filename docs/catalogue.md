# Catalogue

The following table contains the list of all available checks in `cuallee`:

## :material-check-all: Checks 

Check | Description | DataType
 ------- | ----------- | ----
`is_complete` | Zero `nulls` | _agnostic_
`is_empty` | All `nulls` | _agnostic_
`is_unique` | Zero `duplicates` | _agnostic_
`is_primary_key` | Zero `duplicates` | _agnostic_
`are_complete` | Zero `nulls` on group of columns | _agnostic_
`are_unique` | Composite primary key check | _agnostic_
`is_composite_key` | Zero duplicates on multiple columns | _agnostic_
`is_greater_than` | `col > x` | _numeric_
`is_positive` | `col > 0` | _numeric_
`is_negative` | `col < 0` | _numeric_
`is_greater_or_equal_than` | `col >= x` | _numeric_
`is_less_than` | `col < x` | _numeric_
`is_less_or_equal_than` | `col <= x` | _numeric_
`is_equal_than` | `col == x` | _numeric_
`is_contained_in` | `col in [a, b, c, ...]` | _agnostic_
`is_in` | Alias of `is_contained_in` | _agnostic_
`not_contained_in` | `col not in [a, b, c, ...]` | _agnostic_
`not_in` | Alias of `not_contained_in` | _agnostic_
`is_between` | `a <= col <= b` | _numeric, date_
`has_pattern` | Matching a pattern defined as a `regex` | _string_
`is_legit` | String not null & not empty `^\S$` | _string_
`has_min` | `min(col) == x` | _numeric_
`has_max` | `max(col) == x` | _numeric_
`has_std` | `σ(col) == x` | _numeric_
`has_mean` | `μ(col) == x` | _numeric_
`has_sum` | `Σ(col) == x` | _numeric_
`has_percentile` | `%(col) == x` | _numeric_
`has_cardinality` | `count(distinct(col)) == x` | _agnostic_
`has_infogain` | `count(distinct(col)) > 1` | _agnostic_
`has_max_by` | A utilitary predicate for `max(col_a) == x for max(col_b)`  | _agnostic_
`has_min_by` | A utilitary predicate for `min(col_a) == x for min(col_b)`  | _agnostic_
`has_correlation` | Finds correlation between `0..1` on `corr(col_a, col_b)` | _numeric_
`has_entropy` | Calculates the entropy of a column `entropy(col) == x` for classification problems | _numeric_
`is_inside_interquartile_range` | Verifies column values reside inside limits of interquartile range `Q1 <= col <= Q3` used on anomalies.  | _numeric_
`is_in_millions` | `col >= 1e6` | _numeric_
`is_in_billions` | `col >= 1e9` | _numeric_
`is_t_minus_1` | For date fields confirms 1 day ago `t-1` | _date_
`is_t_minus_2` | For date fields confirms 2 days ago `t-2` | _date_
`is_t_minus_3` | For date fields confirms 3 days ago `t-3` | _date_
`is_t_minus_n` | For date fields confirms n days ago `t-n` | _date_
`is_today` | For date fields confirms day is current date `t-0` | _date_
`is_yesterday` | For date fields confirms 1 day ago `t-1` | _date_
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


## :material-gamepad: Controls

Currently available only for `pyspark` and `pandas` dataframes.

Check | Description | DataType
 ------- | ----------- | ----
`completeness` | Zero `nulls` | _agnostic_
`information` | Zero nulls `and` cardinality > 1 | _agnostic_
`intelligence` | Zero nulls, zero empty strings and cardinality > 1 | _agnostic_
`percentage_fill` | `% rows` not empty | _agnostic_
`percentage_empty` | `% rows` empty | _agnostic_


!!! success "Demo"
    ``` py
    import pandas as pd
    from cuallee import Control
    df = pd.DataFrame({"X":[1,2,3], "Y": [10,20,30]})
    # Checks all columns in dataframe for using is_complete check
    Control.completeness(df)
    ```





## :material-earth: ISO
A new module has been incorporated in `cuallee>=0.4.0` which allows the verification of International Standard Organization columns in data frames. Simply access the `check.iso` interface to add the set of checks as shown below.

Check | Description | DataType
 ------- | ----------- | ----
`iso_4217` | currency compliant `ccy` | _string_
`iso_3166` | country compliant `country` | _string_

!!! success "Demo"
    ``` py
    df = spark.createDataFrame([[1, "USD"], [2, "MXN"], [3, "CAD"], [4, "EUR"], [5, "CHF"]], ["id", "ccy"])
    check = Check(CheckLevel.WARNING, "ISO Compliant")
    check.iso.iso_4217("ccy")
    check.validate(df).show()
    +---+-------------------+-------------+-------+------+---------------+--------------------+----+----------+---------+--------------+------+
    | id|          timestamp|        check|  level|column|           rule|               value|rows|violations|pass_rate|pass_threshold|status|
    +---+-------------------+-------------+-------+------+---------------+--------------------+----+----------+---------+--------------+------+
    |  1|2023-05-14 18:28:02|ISO Compliant|WARNING|   ccy|is_contained_in|{'BHD', 'CRC', 'M...|   5|       0.0|      1.0|           1.0|  PASS|
    +---+-------------------+-------------+-------+------+---------------+--------------------+----+----------+---------+--------------+------+
    ```

