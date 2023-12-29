from cuallee.dagster import make_dagster_checks
from cuallee import Check, CheckLevel
import pandas as pd

def test_make_checks():
    df = pd.DataFrame({"id" : [1,2,3,4,5]})
    check = Check(CheckLevel.WARNING, "Dagster")
    check.is_complete("id")
    result = make_dagster_checks(check, "AssetName", df)
    assert isinstance(result, list)
