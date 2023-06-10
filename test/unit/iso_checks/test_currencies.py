import pytest
import pandas as pd

def test_positive(check):
    df = pd.DataFrame({"id" : [1,2,3,4,5], "ccy" : ["USD", "MXN", "CAD", "CHF", "GBP"]})
    check.iso.iso_4217("ccy")
    assert check.validate(df).status.str.match("PASS").all()

def test_negative(check):
    df = pd.DataFrame({"id" : [1,2,3,4,5], "ccy" : ["USD", "MXN", "CAD", "CHF", "111"]})
    check.iso.iso_4217("ccy")
    assert check.validate(df).status.str.match("FAIL").all()