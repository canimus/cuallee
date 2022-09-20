from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from logging import CRITICAL
from operator import attrgetter, methodcaller
import operator as O
import pdb
from shutil import ignore_patterns
from typing import Any, Callable, Collection, List, Union, Tuple, Optional

# from typing_extensions import Self

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
import toolz as Z
from loguru import logger
from pyspark.sql import SparkSession, DataFrame, Observation, Column
from pyspark.sql import Window as W
import uuid
import inspect
import sys
import enum
import itertools as I
from . import dataframe as D
import hashlib


class CheckLevel(enum.Enum):
    WARNING = 0
    ERROR = 1


class CheckTag(enum.Enum):
    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIME = 4


@dataclass(frozen=True)
class Rule:
    method: str
    column: Union[Tuple[str], str]
    value: Optional[Any]
    tag: str
    coverage: float = 1.0


def _single_value_rule(
    column: str,
    value: Optional[Any],
    operator: Callable,
):
    return F.sum((operator(F.col(column), value)).cast("integer"))


class Check:
    def __init__(self, level: CheckLevel, description: str):
        self._rules = []
        self._compute = {}
        self.level = level
        self.description = description

    def is_complete(self, column: str, pct: float = 1.0):
        """Validation for non-null values in column"""
        self._rules.append(Rule("is_complete", column, None, CheckTag.AGNOSTIC, pct))
        self._compute[f"is_complete-{column}-None-{pct}"] = F.sum(
            F.col(column).isNotNull().cast("integer")
        )
        return self

    # computed_alltogether
    def are_complete_1(self, column: Tuple[str], pct: float = 1.0):
        """Validation for non-null values in a group of column"""
        if isinstance(column, List):
            column = tuple(column)
        self._rules.append(Rule("are_complete", column, None, CheckTag.AGNOSTIC, pct))
        self._compute[f"are_complete-{column}-None-{pct}"] = reduce(
            O.add, [F.sum(F.col(c).isNotNull().cast("integer")) for c in column]
        ) / len(column)
        return self

    # computed_individually
    def are_complete_2(self, column: List[str], pct: float = 1.0):
        """Validation for non-null values in a group of column"""
        return [self.is_complete(c, pct) for c in column]

    def is_unique(self, column: str, pct: float = 1.0):
        """Validation for unique values in column"""
        self._rules.append(Rule("is_unique", column, CheckTag.AGNOSTIC, pct))
        self._compute[f"is_unique-{column}-None-{pct}"] = F.count_distinct(
            F.col(column)
        )
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater than value"""
        self._rules.append(
            Rule("is_greater_than", column, value, CheckTag.NUMERIC, pct)
        )
        self._compute[f"is_greater_than-{column}-{value}-{pct}"] = _single_value_rule(
            column, value, O.gt
        )
        return self

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater or equal than value"""
        self._rules.append(
            Rule("is_greater_or_equal_than", column, value, CheckTag.NUMERIC, pct)
        )
        self._compute[
            f"is_greater_or_equal_than-{column}-{value}-{pct}"
        ] = _single_value_rule(column, value, O.ge)
        return self

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less than value"""
        self._rules.append(Rule("is_less_than", column, value, CheckTag.NUMERIC, pct))
        self._compute[f"is_less_than-{column}-{value}-{pct}"] = _single_value_rule(
            column, value, O.lt
        )
        return self

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less or equal than value"""
        self._rules.append(
            Rule("is_less_or_equal_than", column, value, CheckTag.NUMERIC, pct)
        )
        self._compute[
            f"is_less_or_equal_than-{column}-{value}-{pct}"
        ] = _single_value_rule(column, value, O.le)
        return self

    def is_equal(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric column equal than value"""
        self._rules.append(Rule("is_equal", column, value, CheckTag.NUMERIC, pct))
        self._compute[f"is_equal-{column}-{value}-{pct}"] = _single_value_rule(
            column, value, O.eq
        )
        return self

    def has_min(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s minimum value"""
        self._rules.append(Rule("has_min", column, value, CheckTag.NUMERIC))
        self._compute[f"has_min-{column}-{value}-{pct}"] = F.min(F.col(column)) == value
        return self

    def has_max(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s maximum value"""
        self._rules.append(Rule("has_max", column, value, CheckTag.NUMERIC))
        self._compute[f"has_max-{column}-{value}-{pct}"] = F.max(F.col(column)) == value
        return self

    def has_std(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s standard deviation"""
        self._rules.append(Rule("has_std", column, value, CheckTag.NUMERIC))
        self._compute[f"has_std-{column}-{value}-{pct}"] = (
            F.stddev_pop(F.col(column)) == value
        )
        return self

    def __repr__(self):
        return f"Check(level:{self.level}, desc:{self.description}, rules:{len(self._rules)})"

    def validate(self, spark, dataframe: DataFrame):
        """Compute all rules in this check for specific data frame"""
        assert (
            self._rules
        ), "Check is empty. Add validations i.e. is_complete, is_unique, etc."

        assert isinstance(
            dataframe, DataFrame
        ), "Cualle operates only with Spark Dataframes"

        # Pre-validate columns
        rule_set = set(self._rules)
        column_set = set(I.chain.from_iterable(map(attrgetter("column"), rule_set)))
        unknown_columns = column_set.difference(dataframe.columns)
        assert column_set.issubset(
            dataframe.columns
        ), f"Column(s): {unknown_columns} not in dataframe"

        # Pre-Validation of numeric data types
        numeric_rules = set(I.chain.from_iterable([r.column for r in rule_set if r.tag == CheckTag.NUMERIC]))
        numeric_fields = D.numeric_fields(dataframe)
        non_numeric_columns = numeric_rules.difference(numeric_fields)
        assert set(numeric_rules).issubset(
            numeric_fields
        ), f"Column(s): {non_numeric_columns} are not numeric"

        # Create observation object
        observation = Observation(self.description)

        df_observation = dataframe.observe(
            observation,
            *[v.cast(T.StringType()).alias(k) for k, v in self._compute.items()],
        )
        rows = df_observation.count()

        return (
            spark.createDataFrame(
                [k for k in observation.get.items()], ["computed_rule", "results"]
            )
            .withColumn(
                "obs_pct",
                F.when(
                    (F.col("results") == "false") | (F.col("results") == "true"),
                    F.lit(1.0),
                ).otherwise(F.col("results").cast(T.DoubleType()) / rows),
            )
            .withColumn(
                "requiered_pct", F.split(F.col("computed_rule"), "-").getItem(3)
            )
            .select(
                F.lit(self.description).alias("check"),
                F.lit(self.level.name).alias("level"),
                F.split(F.col("computed_rule"), "-").getItem(0).alias("rule"),
                F.split(F.col("computed_rule"), "-").getItem(1).alias("column"),
                F.split(F.col("computed_rule"), "-").getItem(2).alias("value"),
                "results",
                "obs_pct",
                "requiered_pct",
                F.when(
                    (F.col("results") == "true")
                    | (
                        (F.col("results") != "false")
                        & (F.col("obs_pct") >= F.col("requiered_pct"))
                    ),
                    F.lit(True),
                )
                .otherwise(F.lit(False))
                .alias("status"),
            )
        )


def completeness(dataframe: DataFrame) -> float:
    """Percentage of filled values against total number of fields = fields_filled / (rows x cols)"""
    _hash = dataframe.semanticHash()
    observe_completeness = Observation("completeness")
    _pk = f"rows_{_hash}"
    _cols = dataframe.columns
    _nulls = [
        F.sum(F.col(column).isNotNull().cast("long")).alias(f"{column}")
        for column in _cols
    ]
    _rows = [F.count(F.lit(1)).alias(_pk)]
    _null_cols = _nulls + _rows
    dataframe.observe(observe_completeness, *_null_cols).count()

    results = observe_completeness.get
    rows = results.pop(_pk)
    _shape = len(_cols) * rows
    return round(sum(results.values()) / _shape, 6)
