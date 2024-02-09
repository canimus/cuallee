import pytest
import snowflake.snowpark.functions as F  # type: ignore
import pandas as pd
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_cardinality("ID", 10)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_cardinality("ID", 30)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_cardinality("ID", 45, 0.5)
