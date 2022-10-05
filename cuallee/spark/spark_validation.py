from pyspark.sql import SparkSession, DataFrame, Column, Row, Observation
from dataclasses import dataclass
from typing import Dict
from toolz import valfilter  # type: ignore

import operator
import pyspark.sql.functions as F
import pyspark.sql.types as T

from cuallee import Check, Rule, CheckDataType
from cuallee import dataframe as D


@dataclass(frozen=True)
class ComputeInstruction:
    rule: Rule
    expression: Column


class Compute:
    def __init__(self, name: str):
        self._compute: Dict[str, ComputeInstruction] = {}
        self._unique: Dict[str, ComputeInstruction] = {}
        self.name = name
        self.rows = -1

    def __repr__(self):
        return (
            f"Compute(desc:{self.name}, rules:{len({**self._compute, **self._unique})})"
        )

    def is_complete(self, key: str, rule: Rule):
        """Validation for non-null values in column"""
        self._compute[key] = ComputeInstruction(
            rule,
            F.sum(F.col(f"`{rule.column}`").isNotNull().cast("integer")),
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
    if compute._compute:
        observation = Observation(compute.name)

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in compute._compute.items()
            ],
        )
        rows = df_observation.count()
        observation_result = observation.get
    else:
        observation_result = {}
        rows = dataframe.count()

    compute.rows = rows

    unique_observe = (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in compute._unique.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )

    unified_rules = {**compute._unique, **compute._compute}
    unified_results = {**unique_observe, **observation_result}

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "false", F.lit(0.0))
        .when(observed_column == "true", F.lit(1.0))
        .otherwise(observed_column.cast(T.DoubleType()) / compute.rows)  # type: ignore
    )
    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    return (
        spark.createDataFrame(
            [
                Row(  # type: ignore
                    index,
                    compute_instruction.rule.method,
                    str(compute_instruction.rule.column),
                    str(compute_instruction.rule.value),
                    unified_results[hash_key],
                    compute_instruction.rule.coverage,
                )
                for index, (hash_key, compute_instruction) in enumerate(
                    unified_rules.items(), 1
                )
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
