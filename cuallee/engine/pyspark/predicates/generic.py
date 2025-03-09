import operator
from functools import reduce

import pyspark.sql.functions as F

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_complete(rule: Rule):
    """Validation for non-null values in column"""
    column = f"`{rule.column}`"
    condition = F.isnotnull(column) & ~F.isnan(column)
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def are_complete(rule: Rule):
    """Validation for non-null values in a group of columns"""
    column = [f"`{c}`" for c in rule.column]
    condition = [(F.isnotnull(c) & ~F.isnan(c)).cast("bigint") for c in column]
    predicate = (
        reduce(
            operator.add,
            condition,
        )
        == len(rule.column)
    ).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.SELECT,
    )


def is_empty(rule: Rule):
    """Validation for null values in column"""
    column = f"`{rule.column}`"
    condition = F.isnull(column) | F.isnan(column)
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def are_empty(rule: Rule):
    """Validation for null values in a group of columns"""
    column = [f"`{c}`" for c in rule.column]
    condition = [(F.isnull(c) | F.isnan(c)).cast("bigint") for c in column]
    predicate = (
        reduce(
            operator.add,
            condition,
        )
        == len(rule.column)
    ).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.SELECT,
    )


def _apply_approx_count(options: dict):
    instruction = "count_distinct"
    if options and (options.get("approximate", False)):
        instruction = f"approx_{instruction}"
    return instruction


def is_unique(rule: Rule):
    """Validation for unique values in column"""
    column = f"`{rule.column}`"
    instruction = _apply_approx_count(rule.options)
    condition = operator.methodcaller(instruction, F.col(column))(F)
    predicate = None  # F.count_distinct(F.col(rule.column))

    corr_value = F.lit(0)
    if rule.options and (rule.options.get("ignore_nulls", False)):
        corr_value = F.sum((F.isnull(column) | F.isnan(column)).cast("bigint"))

    return ComputeInstruction(
        predicate,
        (condition + corr_value),
        ComputeMethod.SELECT,
    )


def are_unique(rule: Rule):
    """Validation for unique values in a group of columns"""
    column = rule.column
    instruction = _apply_approx_count(rule.options)
    _cols, _corr = [
        (F.col(f"`{c}`"), (F.isnull(f"`{c}`") | F.isnan(f"`{c}`"))) for c in column
    ]
    condition = operator.methodcaller(instruction, *_cols)(F)
    predicate = None

    corr_value = F.lit(0)
    if rule.options and (rule.options.get("ignore_nulls", False)):
        corr_value = F.sum(
            reduce(
                operator.or_,
                _corr,
            ).cast("bigint")
        )

    return ComputeInstruction(
        predicate,
        (condition + corr_value),
        ComputeMethod.SELECT,
    )


def is_between(rule: Rule):
    """Validation of a column between a range"""
    condition = F.col(f"`{rule.column}`").between(*rule.value)
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def not_between(rule: Rule):
    """Validation of a column outside a range"""
    condition = ~F.col(f"`{rule.column}`").between(*rule.value)
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_contained_in(rule: Rule):
    """Validation of column value in set of given values"""
    condition = F.col(f"`{rule.column}`").isin(list(rule.value))
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def not_contained_in(rule: Rule):
    """Validation of column value not in set of given values"""
    condition = ~F.col(f"`{rule.column}`").isin(list(rule.value))
    predicate = condition.cast("bigint")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def satisfies(rule: Rule):
    """Validation of a column satisfying a SQL-like predicate"""
    condition = F.expr(f"{rule.value}").cast("bigint")
    predicate = None
    return ComputeInstruction(
        predicate,
        F.sum(condition),
        ComputeMethod.OBSERVE,
    )
