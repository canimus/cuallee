import operator
from functools import reduce

import pyspark.sql.functions as F

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_complete(rule: Rule):
    """Validation for non-null values in column"""
    predicate = F.col(f"`{rule.column}`").isNotNull().cast("integer")
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
    predicate = None
    return ComputeInstruction(
        predicate,
        F.count_distinct(F.col(f"`{rule.column}`")),
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
