from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_stats_average_value(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_has_mean")
    check.has_mean("ID", 4.5)
    check.config = configurations
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"
