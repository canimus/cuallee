# Introduction

Welcome to `cuallee` and thanks for using this amazing framework. None of this work could have been possible without the inspiration of `pydeequ`. So, thanks to the AWS folks for putting the research work together, and the references so that we could build on the shoulders of giants.

This `pure-python` implementation of unit tests for your data, will help you define validations for your data using `3` concepts described below:


### Entities

To better understand `cuallee` you will need to get familiar with the following 3 concepts: `Check`, `Rule` and `ComputeInstruction`. 

| Entity      | Icon                                 | Description |
| ----------- | :------------------------------------: | ----------- |
| `Check` | :material-check-all:   | Use it to define a group of validations on a `dataframe` and report them as `WARNING` or `ERROR`. You can chain as many rules into a `check`, internally `cuallee` will make sure the same rule is not executed twice. |
| `Rule` | :material-check: | A `rule` represents the _predicate_ you want to test on a `single` or `multiple` columns in a dataframe. A rule as a 4 attributes `method`: name of the predicate, `column`: the column in the dataframe, `value`: the value to compare against and `coverage`: the percentage of positive predicate necessary to set the status of the check to `PASS`.  |
| `ComputeInstruction` | :material-cog: | Are the implementation specific representations of the `predicates` in the `rule`. Because `cuallee` is a dataframe agnostic data quality framework, the implementation of the rules, rely in the creation of compute instructions passed to the specific dataframe of choice, including the following dataframe options: `pandas`, `pyspark` and `snowpark` |

In principle, the only interface you need to be familiar with is the `Check` as it is through this `object` that you can chain your validations and then directly through the `validate` method, execute validations on any `DataFrame`.

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

