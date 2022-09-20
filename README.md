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

### is_complete
```python
from cuallee import Check
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Nulls on column Id
check = Check(CheckLevel.WARNING, "Completeness")
check.is_complete("id").validate(spark, spark.range(10))
```

### is_unique
```python
# Unique values on id
check = Check(CheckLevel.WARNING, "Completeness")
check.is_unique("id").validate(spark, spark.range(10))
```