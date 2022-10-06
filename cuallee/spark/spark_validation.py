from pyspark.sql import SparkSession, DataFrame, Column, Row, Observation
from dataclasses import dataclass
from typing import Dict, Optional, Callable, Any
from toolz import valfilter  # type: ignore
from functools import reduce

import operator
import pyspark.sql.functions as F
import pyspark.sql.types as T

from cuallee import Check, Rule, CheckDataType
from cuallee import dataframe as D


@dataclass(frozen=True)
class ComputeInstruction:
    predicate: Column
    expression: Column


class Compute:
    def __init__(self, name: str):
        self._observe: Dict[str, ComputeInstruction] = {}
        self._unique: Dict[str, ComputeInstruction] = {}
        self._union: Dict[str, ComputeInstruction] = {}
        self.name = name

    def __repr__(self):
        return f"Compute(desc:{self.name}, compute_instructions:{len({**self._observe, **self._unique, **self._union})})"

    def _integrate_compute(self) -> Dict:
        """Unifies the compute dictionaries from observation and select forms"""
        return {**self._observe, **self._unique, **self._union}

    def _single_value_rule(
        self,
        column: str,
        value: Optional[Any],
        operator: Callable,
    ):
        return F.sum((operator(F.col(column), value)).cast("integer"))

    def is_complete(self, key: str, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull()
        self._observe[key] = ComputeInstruction(
            predicate,
            F.sum(predicate.cast("integer")),
        )
        return self

    def are_complete(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for non-null values in a group of columns"""
        predicate = [F.col(f"`{c}`").isNotNull() for c in rule.column]
        self._observe[key] = ComputeInstruction(
            predicate,
            reduce(
                operator.add,
                [
                    F.sum(F.col(f"`{c}`").isNotNull().cast("integer"))
                    for c in rule.column
                ],
            )
            / len(rule.column),
        )
        return self

    def is_unique(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for unique values in column"""
        predicate = F.count_distinct(F.col(rule.column))
        self._unique[key] = ComputeInstruction(
            predicate,
            F.count_distinct(F.col(rule.column)),
        )
        return self

    def are_unique(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for unique values in a group of columns"""
        predicate = F.count_distinct(*[F.col(c) for c in rule.column])
        self._unique[key] = ComputeInstruction(
            predicate,
            F.count_distinct(*[F.col(c) for c in rule.column]),
        )
        return self

    def is_greater_than(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for numeric greater than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.gt)
        self._observe[key] = ComputeInstruction(
            predicate,
            self._single_value_rule(rule.column, rule.value, operator.gt),
        )
        return self

    def is_greater_or_equal_than(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for numeric greater or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.ge)
        self._observe[key] = ComputeInstruction(
            predicate,
            self._single_value_rule(rule.column, rule.value, operator.ge),
        )
        return self

    def is_less_than(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for numeric less than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.lt)
        self._observe[key] = ComputeInstruction(
            predicate,
            self._single_value_rule(rule.column, rule.value, operator.lt),
        )
        return self

    def is_less_or_equal_than(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for numeric less or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.le)
        self._observe[key] = ComputeInstruction(
            predicate,
            self._single_value_rule(rule.column, rule.value, operator.le),
        )
        return self

    def is_equal_than(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for numeric column equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.eq)
        self._observe[key] = ComputeInstruction(
            predicate,
            self._single_value_rule(rule.column, rule.value, operator.eq),
        )
        return self

    def matches_regex(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation for string type column matching regex expression"""
        predicate = F.length(F.regexp_extract(rule.column, rule.value, 0)) > 0
        self._observe[key] = ComputeInstruction(
            predicate,
            F.sum((predicate).cast("integer")),
        )
        return self

    def has_min(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column’s minimum value"""
        predicate = F.min(F.col(rule.column)).eqNullSafe(rule.value)
        self._observe[key] = ComputeInstruction(
            predicate,
            F.min(F.col(rule.column)) == rule.value,
        )
        return self

    def has_max(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column’s maximum value"""
        predicate = F.max(F.col(rule.column)) == rule.value
        self._observe[key] = ComputeInstruction(
            predicate,
            F.max(F.col(rule.column)) == rule.value,
        )
        return self

    def has_std(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column’s standard deviation"""
        predicate = F.stddev_pop(F.col(rule.column)) == rule.value
        self._observe[key] = ComputeInstruction(
            predicate,
            F.stddev_pop(F.col(rule.column)) == rule.value,
        )
        return self

    def has_mean(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column's average/mean"""
        predicate = F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value)
        self._observe[key] = ComputeInstruction(
            predicate,
            F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        )
        return self

    def is_between(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column between a range"""
        predicate = F.col(rule.column).between(*rule.value).cast("integer")
        self._observe[key] = ComputeInstruction(
            predicate,
            F.sum(predicate),  # type: ignore
        )
        return self

    def is_contained_in(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of column value in set of given values"""
        predicate = F.col(rule.column).isin(list(rule.value))
        self._observe[key] = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.LongType())),
        )
        return self

    def has_percentile(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column percentile value"""
        predicate = F.percentile_approx(
            F.col(f"`{rule.column}`").cast(T.DoubleType()), rule.value[1], rule.value[2]
        ).eqNullSafe(rule.value[0])
        self._unique[key] = ComputeInstruction(
            predicate,
            F.percentile_approx(
                F.col(f"`{rule.column}`").cast(T.DoubleType()),
                rule.value[1],
                rule.value[2],
            ).eqNullSafe(rule.value[0]),
        )
        return self

    def has_max_by(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column maximum based on other column maximum"""
        predicate = F.max_by(rule.column[1], rule.column[0]) == rule.value
        self._observe[key] = ComputeInstruction(
            predicate,
            F.max_by(rule.column[1], rule.column[0]) == rule.value,
        )
        return self

    def has_min_by(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column minimum based on other column minimum"""
        predicate = F.min_by(rule.column[1], rule.column[0]) == rule.value
        self._observe[key] = ComputeInstruction(
            predicate,
            F.min_by(rule.column[1], rule.column[0]) == rule.value,
        )
        return self

    def has_correlation(self, key: str, rule: Rule):  # To Do with Predicate
        """Validates the correlation between 2 columns with some tolerance"""
        predicate = F.corr(
            F.col(f"`{rule.column[0]}`").cast(T.DoubleType()),
            F.col(f"`{rule.column[1]}`").cast(T.DoubleType()),
        ).eqNullSafe(F.lit(rule.value))
        self._unique[key] = ComputeInstruction(
            predicate,
            F.corr(
                F.col(f"`{rule.column[0]}`").cast(T.DoubleType()),
                F.col(f"`{rule.column[1]}`").cast(T.DoubleType()),
            ).eqNullSafe(F.lit(rule.value)),
        )
        return self

    def satisfies(self, key: str, rule: Rule):  # To Do with Predicate
        """Validation of a column satisfying a SQL-like predicate"""
        predicate = F.expr(rule.value).cast("integer")
        self._observe[key] = ComputeInstruction(
            predicate,
            F.sum(F.expr(rule.value).cast("integer")),
        )
        return self


def compute_summary(
    spark: SparkSession, dataframe: DataFrame, check: Check
) -> DataFrame:
    """Compute all rules in this check for specific data frame"""

    # Create expression dictionnaries
    compute = Compute(check.name)
    for k, v in check._rule.items():
        f = operator.methodcaller(v.method, k, v)
        f(compute)

    # Pre-Validation of numeric data types
    _col = operator.attrgetter("column")
    _numeric = lambda x: x.data_type.name == CheckDataType.NUMERIC.name
    _date = lambda x: x.data_type.name == CheckDataType.DATE.name
    _timestamp = lambda x: x.data_type.name == CheckDataType.TIMESTAMP.name
    _string = lambda x: x.data_type.name == CheckDataType.STRING.name
    assert set(
        Check._compute_columns(
            map(_col, valfilter(_numeric, check._rule).values())  # type: ignore
        )
    ).issubset(D.numeric_fields(dataframe))
    assert set(
        Check._compute_columns(
            map(_col, valfilter(_string, check._rule).values())  # type: ignore
        )
    ).issubset(D.string_fields(dataframe))
    assert set(
        Check._compute_columns(map(_col, valfilter(_date, check._rule).values()))  # type: ignore
    ).issubset(D.date_fields(dataframe))
    assert set(
        Check._compute_columns(
            map(_col, valfilter(_timestamp, check._rule).values())  # type: ignore
        )
    ).issubset(D.timestamp_fields(dataframe))

    # Compute the expression
    if compute._observe:
        observation = Observation(compute.name)

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in compute._observe.items()
            ],
        )
        rows = df_observation.count()
        observation_result = observation.get
    else:
        observation_result = {}
        rows = dataframe.count()

    unique_result = (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in compute._unique.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )

    union_result = (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in compute._union.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )
    
    unified_results = {**observation_result, **unique_result, **union_result}

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "false", F.lit(0.0))
        .when(observed_column == "true", F.lit(1.0))
        .otherwise(observed_column.cast(T.DoubleType()) / rows)  # type: ignore
    )
    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    return (
        spark.createDataFrame(
            [
                Row(  # type: ignore
                    index,
                    rule.method,
                    str(rule.column),
                    str(rule.value),
                    unified_results[hash_key],
                    rule.coverage,
                )
                for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
            ],
            schema="id int, rule string, column string, value string, result string, pass_threshold string",
        )
        .select(
            F.col("id"),
            F.lit(check.date.strftime("%Y-%m-%d")).alias("date"),
            F.lit(check.date.strftime("%H:%M:%S")).alias("time"),
            F.lit(check.name).alias("check"),
            F.lit(check.level.name).alias("level"),
            F.col("column"),
            F.col("rule"),
            F.col("value"),
            F.lit(rows).alias("rows"),
            _calculate_pass_rate(F.col("result")).alias("pass_rate"),
            F.col("pass_threshold").cast(T.DoubleType()),
        )
        .withColumn(
            "status",
            _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
        )
    )


def malformed_records() -> DataFrame:
    return "I am a malformed record"
