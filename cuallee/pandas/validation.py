import pandas as pd

from cuallee import Check, dataframe

def compute_summary(dataframe: pd.DataFrame, check: Check) -> pd.DataFrame:
    """Compute all rules in this check for specific data frame"""
    return f'I am a Pandas DataFrame!'