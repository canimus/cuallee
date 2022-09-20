# cuallee
Meaning `good` in Aztec (Nahuatl)


This library provides an intuitive `API` to describe `checks` for Apache PySpark DataFrames.
It is a replacement written in pure `python` of the `pydeequ` framework.


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