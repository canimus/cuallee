# Introduction

Welcome to `cuallee` and thanks for using this amazing framework. None of this work could have been possible without the inspiration of `pydeequ`. So, thanks to the AWS folks for putting the research work together, and the references so that we could build on the shoulders of giants.

This `pure-python` implementation of unit tests for your data, will help you define validations for your data using `3` concepts described below:


## Entities

To better understand `cuallee` you will need to get familiar with the following 3 concepts: `Check`, `Rule` and `ComputeInstruction`. 

| Entity      | Icon                                 | Description |
| ----------- | :------------------------------------: | ----------- |
| `Check` | :material-check-all:   | Use it to define a group of validations on a `dataframe` and report them as `WARNING` or `ERROR`. You can chain as many rules into a `check`, internally `cuallee` will make sure the same rule is not executed twice. |
| `Rule` | :material-check: | A `rule` represents the _predicate_ you want to test on a `single` or `multiple` columns in a dataframe. A rule as a 4 attributes `method`: name of the predicate, `column`: the column in the dataframe, `value`: the value to compare against and `coverage`: the percentage of positive predicate necessary to set the status of the check to `PASS`.  |
| `Control` | :material-gamepad: | Currently only supported in `pyspark` it allows to run pre-fabricated checks that target all columns in a dataframe. Controls help a user to run validation faster, without having to add individual rules for each column on a dataframe. For example `Control.completeness` creates a Check, and then adds `is_complete` rules to each column of the dataframe |

In principle, the only interface you need to be familiar with is the `Check` as it is through this `object` that you can chain your validations and then directly through the `validate` method, execute validations on any `DataFrame`.

## Process
`cuallee` is designed to rapidly develop quality rules for your data.<br/>The process consists in the following :material-numeric-4-circle-outline: steps:
``` mermaid
graph LR
  
  A[Create Check] -.-> B[Add Rules];
  B[Add Rules] -.-> C[Load Data];
  C[Load Data] -.-> D[Validate DataFrame];

```

1. __Create Check__: Consists in calling the `check = Check()` constructor to initiate a validation
2. __Add_Rules__: consists in calling any of the rules available in the check, providing a `column` and when required a expected `value`. For example: `check.is_unique(id)`
3. __Load Data__: Is the process of creating a dataframe normally by loading data from your data sources, by convention in the docs we refer to dataframes as `df`. <br/>For example in `pandas` :material-arrow-right-bold-circle: `df = pd.read_csv("data.csv")`
4. __Validate DataFrame__: Consists in running `check.validate(df)`

## History

Why did we create a software that offers functionality similar to `pydeequ`, `great-expectations`, and `soda-core`? While these tools are inspiring and have lowered the barrier for better data quality in the complex ecosystem of data platforms, they are primarily geared towards enterprise, collaborative, and licensed-supported software.

Cuallee's operational model, from day one, aimed to be a transparent, reliable, and robust solution for large-scale data sources and team profiles. It focuses on portability and ease of use. Differing from the `low-code` movement that foster authoring data quality checks without coding experience or through configuration formats like YAML or proprietary meta-languages.

Cuallee championed the idea of maintaining a code-centric approach to quality definitions, focusing on areas where data defects are statistically more likely to appear. This includes the logistics of moving data and the transformation phase, where business logic is needed to process original sources. This approach aligns with the concept of safeguarding data through its life-cycle via unit tests, as originally proposed by the deequ framework.

There were three main reasons for cuallee becoming a standalone solution rather than just a contribution to the pydeequ framework:

- __Scala development__: Cuallee is written entirely in Python, which benefits from a larger development ecosystem, especially considering Python's dominance in the data ecosystem.
- __Computation model__: The architecture of initializing a pydeequ session encountered friction during the implementation of quality rules, influencing memory footprint and requiring additional jar files for use in PySpark.
- __Extensibility to other data structures__: Cuallee addresses the widely adopted workflow for data experimentation, where teams often start their journeys in `notebooks` or local environments before transitioning to packaged software releases, subject to more stringent criteria like vulnerability management, release management, quality control, telemetry, etc. Plus, the transition to `out-of-memory` frameworks like PySpark for distributed execution.


In summary, `cuallee` provides a Python-centric, transparent, and robust solution for data quality testing, particularly suited for teams operating in hybrid environments or undergoing technology migrations.