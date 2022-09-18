from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from logging import CRITICAL
from operator import attrgetter, methodcaller
import pdb
from typing import Any, Collection, List, Union, Tuple, Optional
from typing_extensions import Self

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import toolz as Z
from loguru import logger
from pyspark.sql import SparkSession, DataFrame, Observation
from pyspark.sql import Window as W
import uuid
import inspect
import sys
import enum
import itertools
from . import dataframe as D


class CheckLevel(enum.Enum):
    WARNING = 0
    ERROR = 1


class CheckTag(enum.Enum):
    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2


@dataclass(frozen=True)
class Rule:
    method: str
    column: List[str]
    value: Optional[Any]
    expression: str
    tag: str
    coverage: float = 1.0

    def __repr__(self):
        return f"""
method: {self.method}
column: {self.column}
expression: {self.expression}
        """.stip()


class Check:
    def __init__(self, level: CheckLevel, description: str):
        self._rules = []
        self._compute = {}
        self.level = level
        self.description = description

    def is_complete(self, column: str) -> Self:
        """
        Validation for non-null values in column
        ƒ: F.sum(F.col(column).isNotNull().cast('integer'))
        """
        self._rules.append(Rule("is_complete", column))
        return self

    def is_unique(self, column: str) -> Self:
        """Validation for unique values in column"""
        self._rules.append(Rule("is_unique", column))
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0) -> Self:
        """
        Validation for numeric greater than value
        ƒ: F.sum((F.col(column) > value).cast('integer'))
        """
        self._rules.append(
            Rule(
                method="is_greater_than",
                column=column,
                value=value,
                expression=lambda rows, expectation: (
                    (F.sum((F.col(column) > value).cast("integer")) / F.lit(rows))
                    >= F.lit(expectation)
                ).alias(f"is_greater_than_{column}"),
                tag=CheckTag.NUMERIC,
                coverage=pct,
            )
        )
        return self

    def is_less_than(self, column: str, value: float, pct: float = 1.0) -> Self:
        """
        Validation for numeric greater than value
        ƒ: F.sum((F.col(column) > value).cast('integer'))

        """
        self._rules.append(
            Rule(
                method="is_less_than",
                column=column,
                value=value,
                expression=lambda rows, expectation: (
                    (F.sum((F.col(column) < value).cast("integer")) / F.lit(rows))
                    >= F.lit(expectation)
                ).alias(f"is_less_than_{column}"),
                tag=CheckTag.NUMERIC,
                coverage=pct,
            )
        )
        return self

    # HERE Thank you!
    # ==============
    def has_min(self, column: str, value: float) -> Self:
        """Validation of a column’s minimum value"""
        self._rules.append(Rule("has_min", column, value, CheckTag.NUMERIC))
        return self
        # Calculation:
        # assert F.min(F.col(column)) == value

    def has_max(self, column: str, value: float) -> Self:
        """Validation of a column’s maximu value"""
        pass

    # ==============

    def __repr__(self):
        return f"""
description: {self.description}
level: {self.level}
rules: {len(self._rules)}
""".strip()

    def validate(self, dataframe: DataFrame):
        """Compute all rules in this check for specific data frame"""
        assert isinstance(
            dataframe, DataFrame
        ), "Cualle operates only with Spark Dataframes"

        # Pre-validate columns
        rule_set = set(self._rules)
        column_set = set(map(attrgetter("column"), rule_set))
        unknown_columns = column_set.difference(dataframe.columns)
        assert column_set.issubset(
            dataframe.columns
        ), f"Column(s): {unknown_columns} not in dataframe"

        # Pre-Validation of numeric data types
        numeric_rules = set([r.column for r in rule_set if r.tag == CheckTag.NUMERIC])
        numeric_fields = D.numeric_fields(dataframe)
        non_numeric_columns = numeric_rules.difference(numeric_fields)
        assert set(numeric_rules).issubset(
            numeric_fields
        ), f"Column(s): {non_numeric_columns} are not numeric"

        rows = dataframe.count()
        return dataframe.select(*[r.expression(rows, r.coverage) for r in rule_set])

        # 2. DataType


# def numeric_fields(dataframe : DataFrame) -> List[str]:
#         """ Filter all numeric data types in data frame and returns field names """
#         return set([f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.NumericType)])


def _compute_distribution(observation: dict) -> bool:
    logger.info(observation)
    return observation["positive"] / observation["rows"] == 1.0


def is_complete(dataframe: DataFrame, column: str) -> bool:
    observe_completeness = Observation(f"is_complete({column})")
    dataframe.observe(
        observe_completeness,
        F.sum(F.col(column).isNull().cast("long")).alias(f"negative"),
        F.sum(F.col(column).isNotNull().cast("long")).alias(f"positive"),
        F.count(F.lit(1)).alias("rows"),
    ).count()

    return _compute_distribution(observe_completeness.get)


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


def uniqueness(dataframe: DataFrame, exclude_nulls=False) -> float:
    """Percentage of unique values from entire data set as compound from column"""
    dataframe.select([F.sum(F.count_distinct(c)).alias(c) for c in dataframe.columns])


def is_unique(dataframe: DataFrame, column: str) -> bool:
    ratio = (
        dataframe.select(
            F.expr(
                f"""
                count(distinct({column})) / count(1) as positive
                """
            )
        )
        .first()
        .positive
    )
    logger.info(f"Distribution of is_unique: {ratio}")
    return ratio == 1.0


def is_contained(
    dataframe: DataFrame, column: str, collection: Collection[Any], ignore_nulls=False
) -> bool:
    logger.info(f"{column} is_contained in {collection}")
    positive_cases = dataframe.select(
        F.col(column).isin(collection).alias("positive")
    ).collect()

    results = map(attrgetter("positive"), positive_cases)
    if ignore_nulls:
        results = filter(None, results)

    return all(results)
