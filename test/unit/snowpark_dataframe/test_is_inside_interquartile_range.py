import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_inside_interquartile_range(snowpark, configurations):
    df = snowpark.range(16)
    check = Check(CheckLevel.WARNING, "check_is_inside_interquartile_range")
    check.is_inside_interquartile_range("ID")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 8
