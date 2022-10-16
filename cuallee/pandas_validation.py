from typing import Dict
from cuallee import Check, Rule
import pandas as pd


def compute(rules: Dict[str, Rule]):
    return {}


def validate_data_types(rules: Dict[str, Rule], dataframe: pd.DataFrame):
    return True


def summary(check: Check, dataframe: pd.DataFrame):
    return pd.DataFrame({"status": ["OK"]})
