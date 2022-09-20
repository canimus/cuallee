# cuallee
Meaning `good` in Aztec (Nahuatl)


This library provides an intuitive `API` to describe `checks` for Apache PySpark DataFrames `v3.3.0`.
It is a replacement written in pure `python` of the `pydeequ` framework.

I gave up in _deequ_ as project does not seem to be maintained, and the multiple issues with the callback server.

## Advantages
This implementation goes in hand with the latest API from PySpark and uses the `Observation` API to collect metrics
at the lower cost of computation. 
When benchmarking against pydeequ, `cuallee` uses circa <3k java classes underneath and **remarkably** less memory.
 
> __cuallee__ is inpired by the Green Software Foundation principles, on the advantages of `green` software.


## Checks

### Completeness and Uniqueness
```python
from cuallee import Check
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Nulls on column Id
check = Check(CheckLevel.WARNING, "Completeness")
(
    check
    .is_complete("id")
    .is_unique("id")
    .validate(spark, df)
).show() # Returns a pyspark.sql.DataFrame
```

### Date Algebra
```python
# Unique values on id
check = Check(CheckLevel.WARNING, "CheckIsBetweenDates")
df = spark.sql("select explode(sequence(to_date('2022-01-01'), to_date('2022-01-10'), interval 1 day)) as date")
assert (
    check.is_between("date", "2022-01-01", "2022-01-10")
    .validate(spark, df)
    .first()
    .status
)
```

### Value Membership
```python
df = spark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["ID", "value"])
check = Check(CheckLevel.WARNING, "is_contained_in_number_test")
check.is_contained_in("value", (10, 15, 20, 25)).validate(spark, df)
```

### Regular Expressions
```python
df = spark.createDataFrame([[1, "is_blue"], [2, "has_hat"], [3, "is_smart"]], ["ID", "desc"])
check = Check(CheckLevel.WARNING, "matches_regex_test")
check.matches_regex("desc", r"^is.*t$") # only match is_smart 33% of rows.
check.validate(spark, df).first().status == "FAIL"
```

### Real Usage
```python
check = Check(CheckLevel.ERROR, "IndexPrices")
(
    check
    .is_complete("BusinessDateTime")
    .is_complete("CMAEntityId")
    .is_complete("CMATicker")
    .is_complete("EntityName")
    .is_complete("Region")
    .is_complete("Seniority")
    .is_complete("Currency")
    .is_complete("RestructuringType")
    .is_complete("InstrumentType")
    .is_complete("Tenor")
    .is_complete("MaturityDate")
    .is_complete("MarketQuotingConvention")
    .is_complete("ObservedDerivedIndicator")
    .is_complete("Coupon")
    .is_complete("MarketRecoveryRate")
    .is_unique("CMATicker")
    .is_contained_in("Seniority", ["Senior", "SeniorLAC", "Subordinated"])
    .is_contained_in("InstrumentType", ["Index", "Single Name", "Tranche"])
    .is_contained_in("MarketQuotingConvention", ["PercentOfPar", "QuoteSpread", "Upfront"])
    .is_contained_in("ObservedDerivedIndicator", ["D", "O"])
    .is_between("Coupon", 25, 500)
    .is_between("MarketRecoveryRate", 0, 100)
    .is_between("Tenor", 0, 30)
    validate(spark, df)
).show(truncate=False)
+---+----------+--------+----------+-------+------------------------+---------------+------------------------------------------+-----+---------+--------------+------+
|id |date      |time    |check     |level  |column                  |rule           |value                                     |rows |pass_rate|pass_threshold|status|
+---+----------+--------+----------+-------+------------------------+---------------+------------------------------------------+-----+---------+--------------+------+
|1  |2022-09-21|01:05:51|CdsPricing|WARNING|CMATicker               |is_unique      |N/A                                       |42462|0.06     |1.0           |FAIL  |
|2  |2022-09-21|01:05:51|CdsPricing|WARNING|MaturityDate            |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|3  |2022-09-21|01:05:51|CdsPricing|WARNING|MarketRecoveryRate      |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|4  |2022-09-21|01:05:51|CdsPricing|WARNING|InstrumentType          |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|5  |2022-09-21|01:05:51|CdsPricing|WARNING|CMATicker               |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|6  |2022-09-21|01:05:51|CdsPricing|WARNING|Seniority               |is_contained_in|('Senior', 'SeniorLAC', 'Subordinated')   |42462|1.0      |1.0           |PASS  |
|7  |2022-09-21|01:05:51|CdsPricing|WARNING|MarketQuotingConvention |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|8  |2022-09-21|01:05:51|CdsPricing|WARNING|Region                  |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|9  |2022-09-21|01:05:51|CdsPricing|WARNING|Coupon                  |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|10 |2022-09-21|01:05:51|CdsPricing|WARNING|BusinessDateTime        |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|11 |2022-09-21|01:05:51|CdsPricing|WARNING|InstrumentType          |is_contained_in|('Index', 'Single Name', 'Tranche')       |42462|1.0      |1.0           |PASS  |
|12 |2022-09-21|01:05:51|CdsPricing|WARNING|ObservedDerivedIndicator|is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|13 |2022-09-21|01:05:51|CdsPricing|WARNING|Coupon                  |is_between     |(25, 500)                                 |42462|1.0      |1.0           |PASS  |
|14 |2022-09-21|01:05:51|CdsPricing|WARNING|EntityName              |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|15 |2022-09-21|01:05:51|CdsPricing|WARNING|MarketRecoveryRate      |is_between     |(0, 100)                                  |42462|1.0      |1.0           |PASS  |
|16 |2022-09-21|01:05:51|CdsPricing|WARNING|Tenor                   |is_between     |(0, 30)                                   |42462|1.0      |1.0           |PASS  |
|17 |2022-09-21|01:05:51|CdsPricing|WARNING|RestructuringType       |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|18 |2022-09-21|01:05:51|CdsPricing|WARNING|ObservedDerivedIndicator|is_contained_in|('D', 'O')                                |42462|1.0      |1.0           |PASS  |
|19 |2022-09-21|01:05:51|CdsPricing|WARNING|Tenor                   |is_complete    |N/A                                       |42462|1.0      |1.0           |PASS  |
|20 |2022-09-21|01:05:51|CdsPricing|WARNING|MarketQuotingConvention |is_contained_in|('PercentOfPar', 'QuoteSpread', 'Upfront')|42462|1.0      |1.0           |PASS  |
+---+----------+--------+----------+-------+------------------------+---------------+------------------------------------------+-----+---------+--------------+------+
```

### More...
- `are_complete(*cols)`
- `matches_regex(col, regex)`
- `is_greater_than(col, val)`
- `is_greater_or_equal_than(col, val)`
- `is_less_than(col, val)`
- `is_less_or_equal_than(col, val)`
- `is_equal_than(col, val)`
- `has_min(col, val)`
- `has_max(col, val)`
- `has_std(col, val)`
- `has_percentile(col, value, percentile, precision, coverage)`
- `is_between(col, i, k)`
- `is_between(col, date_1, date_2)`
- `has_min_by(col2, col1, value)`
- `satisfies(predicate, coverage)`