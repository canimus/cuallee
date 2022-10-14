import pandas as pd  # type: ignore

from cuallee import Check


def pd_compute_summary(dataframe: pd.DataFrame, check: Check) -> pd.DataFrame:
    """Compute all rules in this check for specific data frame"""
    return "I am a Pandas DataFrame!"
