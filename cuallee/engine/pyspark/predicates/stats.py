from typing import Any, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def has_min(rule: Rule):
    """Validation of a column's minimum value"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.min(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        ComputeMethod.OBSERVE,
    )


def has_max(rule: Rule):
    """Validation of a column's maximum value"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.max(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        ComputeMethod.OBSERVE,
    )


def has_mean(rule: Rule):
    """Validation of a column's average/mean"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        ComputeMethod.OBSERVE,
    )


def has_std(rule: Rule):
    """Validation of a column's standard deviation"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.stddev_pop(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        ComputeMethod.SELECT,
    )


def has_sum(rule: Rule):
    """Validation of a column's sum of values"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.sum(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
        ComputeMethod.OBSERVE,
    )


def has_percentile(rule: Rule):
    """Validation of a column percentile value"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.percentile_approx(
            F.col(f"`{rule.column}`").cast("double"),
            rule.settings["percentile"],
            rule.settings["precision"],
        ).eqNullSafe(rule.value),
        ComputeMethod.SELECT,
    )


def is_inside_interquartile_range(rule: Rule):
    """Validates a number resides inside the Q3 - Q1 range of values"""
    predicate = None

    def _execute(dataframe: Union[DataFrame, Any], key: str):
        return dataframe.select(F.lit(True).alias(key))

    return ComputeInstruction(
        predicate,
        _execute,
        ComputeMethod.TRANSFORM,
    )
