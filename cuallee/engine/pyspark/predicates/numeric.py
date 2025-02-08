import operator

import pyspark.sql.functions as F

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_greater_than(rule: Rule):
    """Validation for numeric greater than value"""
    predicate = operator.gt(F.col(f"`{rule.column}`"), rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_greater_or_equal_than(rule: Rule):
    """Validation for numeric greater or equal than value"""
    predicate = operator.ge(F.col(f"`{rule.column}`"), rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_less_than(rule: Rule):
    """Validation for numeric less than value"""
    predicate = operator.lt(F.col(f"`{rule.column}`"), rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_less_or_equal_than(rule: Rule):
    """Validation for numeric less or equal than value"""
    predicate = operator.le(F.col(f"`{rule.column}`"), rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )


def is_equal_than(rule: Rule):
    """Validation for numeric column equal than value"""
    predicate = operator.eq(F.col(f"`{rule.column}`"), rule.value).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )
