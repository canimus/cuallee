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
