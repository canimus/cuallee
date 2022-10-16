import enum
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from typing import Union, Dict
from dataclasses import dataclass
from snowflake.snowpark import DataFrame, Column

from cuallee import Check, Rule


class ComputeMethod(enum.Enum):
    SELECT = 'SELECT'
    TRANSFORM = 'TRANSFORM'


@dataclass
class ComputeInstruction:
    predicate: Union[Column, None]
    expression: Column
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Compute:
    def __init__(self):
        self.compute_instruction = ComputeInstruction

    def __repr__(self):
        return self.compute_instruction

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction


def compute(rules: Dict[str, Rule]):
    return 'Compute dictionnary'

def validate_data_types(rules: Dict[str, Rule], dataframe: DataFrame):
    return 'I validate DataTypes'

def summary(check: Check, dataframe: DataFrame):
    """Compute all rules in this check for specific data frame"""
    return "I am a Snow DataFrame!"

