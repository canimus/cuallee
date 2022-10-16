import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from typing import Dict
from snowflake.snowpark import DataFrame

from cuallee import Check, Rule


def compute(rules: Dict[str, Rule]):
    return 'Compute dictionnary'

def validate_data_types(rules: Dict[str, Rule], dataframe: DataFrame):
    return 'I validate DataTypes'

def summary(check: Check, dataframe):
    """Compute all rules in this check for specific data frame"""
    return "I am a Snow DataFrame!"

