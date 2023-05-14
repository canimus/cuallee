import pytest
import pandas as pd

def test_positive(check):
    df = pd.DataFrame({"id" : [1,2,3,4,5], "country" : ["US", "ES", "MX", "NZ", "AU"]})
    check.iso.iso_3166("country")
    assert check.validate(df).status.str.match("PASS").all()

def test_negative(check):
    df = pd.DataFrame({"id" : [1,2,3,4,5], "country" : ["US", "ES", "MX", "NZ", "00"]})
    check.iso.iso_3166("country")
    assert check.validate(df).status.str.match("FAIL").all()