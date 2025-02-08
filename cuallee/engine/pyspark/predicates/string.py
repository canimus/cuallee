import pyspark.sql.functions as F

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def has_pattern(rule: Rule):
    """Validation for string type column matching regex expression"""
    predicate = (
        F.length(F.regexp_extract(F.col(f"`{rule.column}`"), f"{rule.value}", 0)) > 0
    ).cast("integer")
    return ComputeInstruction(
        predicate,
        F.sum(predicate),
        ComputeMethod.OBSERVE,
    )
