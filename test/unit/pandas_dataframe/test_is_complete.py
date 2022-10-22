import pandas as pd
from cuallee import Check, CheckLevel


def test_natural_numer():
    check = Check(CheckLevel.WARNING, "Completeness")
    check.is_complete("id")
    df = pd.DataFrame({"id": [10, 20, 30]})
    assert check.validate(df).status.eq("PASS").all()
