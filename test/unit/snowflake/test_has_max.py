from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_stats_max_value(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_has_max")
    check.has_max("ID", 9.0)
    check.config = configurations
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_stats_max_value_int(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_has_max_int")
    check.has_max("ID", 9)
    check.config = configurations
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"
