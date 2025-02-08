import pyspark.sql.functions as F

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
