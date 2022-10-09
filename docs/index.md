# Introduction

Welcome to `cuallee` and thanks for using this amazing framework. None of this work could have been possible without the inspiration of `pydeequ`. So, thanks to the AWS folks for putting the research work together, and the references so that we could build on the shoulders of giants.

This `pure-python` implementation of unit tests for your data, will help you define validations for your data using `3` concepts described below:


### Entities used

| Entity      | Icon                                 | Description |
| ----------- | :------------------------------------: | ----------- |
| `Check` | :material-check-all:   | Use to define `CheckLevel` and a brief description |
| `Rule` | :material-check: | Added to the `Check` with individual predicates for validation |
| `ComputeInstruction` | :material-cog: | Created internally in the check to run against `pyspark` dataframes |

In principle, the only interface you need to be familiar with is the `Check` as it is through this `object` that you can chain your validations and then directly through the `validate` method, execute validations on `DataFrame` loaded through `pyspark`.

## Process Flow
``` mermaid
graph LR
  U((start)) --> A;
  A[Check] -.-> B(is_complete);
  A[Check] -.-> C(is_between);
  A[Check] -.-> D(is_on_weekday);
  B --> E{all rules?};
  C --> E{all rules?};
  D --> E{all rules?};
  E --> F[/read dataframe/];
  A -.-> G{want results?};
  F --> G;
  G --> H(validate);
  H --> I([get results])
  I --> K((end))

```

## Installation

`cuallee` is designed to work with `pyspark==3.3.0` and this is its only dependency.
The __API__ uses the `Observation` features in pyspark, to reduce the computation time for aggregations, and calculating summaries in one pass of the data frames being validated.

## pip

```bash
# Latest
pip install cuallee
```

## Check

Validating data sets is about creating a `Check` and adding rules into it.
You can choose from different types: `numeric`, `date algebra`, `range of values`, `temporal`, and many others.

A `Check` provides a declarative interface to build a comprehensive validation on a dataframe as shown below:

```python
# Imports
from cuallee import Check, CheckLevel
from cuallee import dataframe as D

# Check 
check = Check(CheckLevel.WARNING, "TaxiNYCheck")

# Data
df = spark.read.parquet("temp/taxi/*.parquet")

# Adding rules
# =============

 # All fields are filled
[check.is_complete(name) for name in df.columns]

# Verify taxi ride distance is positive
[check.is_greater_than(name, 0) for name in D.numeric_fields(df)] 

# Confirm that tips are not outliers
[check.is_less_than(name, 1e4) for name in D.numeric_fields(df)] 

# 70% of data is on weekdays
[check.is_on_weekday(name, .7) for name in D.timestamp_fields(df)] 

# Binary classification fields
[check.has_entropy(name, 1.0, 0.5) for name in D.numeric_fields(df)] 

# Percentage of big tips
[check.is_between(name, (1000,2000)) for name in D.numeric_fields(df)] 

# Confirm 22 years of data
[check.is_between(name, ("2000-01-01", "2022-12-31")) for name in D.timestamp_fields(df)] 

```