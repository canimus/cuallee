from pyspark.sql import SparkSession, DataFrame, Column, Row, Observation
from dataclasses import dataclass
from typing import Dict

import operator
import pyspark.sql.functions as F
import pyspark.sql.types as T

from cuallee import Check, Rule

import logging

logger = logging.getLogger(__name__)


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

    compute = Compute(check.name)
    for k, v in check._rule.items():
        f = operator.methodcaller(v.method, k, v)
        f(compute)

    if compute._compute:
        observation = Observation(compute.name)
        for k, v in compute._compute.items():
            logger.info(str(v.expression))

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in compute._compute.items()
            ],
        )
        rows = df_observation.count()
        observation_result = observation.get
        logger.info(observation_result)
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
