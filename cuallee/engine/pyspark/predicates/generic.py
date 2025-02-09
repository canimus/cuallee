import operator
from functools import reduce

import pyspark.sql.functions as F

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_complete(rule: Rule):
    """Validation for non-null values in column"""
    column = f"`{rule.column}`"
    condition = F.isnotnull(column) & ~F.isnan(column)
    predicate = condition.cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_empty(rule: Rule):
    """Validation for null values in column"""
    predicate = F.col(f"`{rule.column}`").isNull().cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def are_complete(rule: Rule):
    """Validation for non-null values in a group of columns"""
    predicate = (
        reduce(
            operator.add,
            [F.col(f"`{c}`").isNotNull().cast("integer") for c in rule.column],
        )
        == len(rule.column)
    ).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.SELECT,
    )


def is_unique(rule: Rule):
    """Validation for unique values in column"""
    predicate = None  # F.count_distinct(F.col(rule.column))
    instruction = "count_distinct"
    if rule.options and (rule.options.get("approximate", False)):
        instruction = f"approx_{instruction}"

    return ComputeInstruction(
        predicate,
        operator.methodcaller(instruction, F.col(f"`{rule.column}`"))(F),
        ComputeMethod.SELECT,
    )


def are_unique(rule: Rule):
    """Validation for unique values in a group of columns"""
    predicate = None
    return ComputeInstruction(
        predicate,
        F.count_distinct(*[F.col(c) for c in rule.column]),
        ComputeMethod.SELECT,
    )


def is_between(rule: Rule):
    """Validation of a column between a range"""
    predicate = F.col(f"`{rule.column}`").between(*rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_contained_in(rule: Rule):
    """Validation of column value in set of given values"""
    predicate = F.col(f"`{rule.column}`").isin(list(rule.value)).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def not_contained_in(rule: Rule):
    """Validation of column value not in set of given values"""
    predicate = ~F.col(f"`{rule.column}`").isin(list(rule.value))
    return ComputeInstruction(
        predicate,
        F.sum(predicate.cast("long")),
        ComputeMethod.OBSERVE,
    )
