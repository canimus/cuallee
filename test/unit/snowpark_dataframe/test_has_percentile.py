import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame, Row  # type: ignore
from cuallee import Check, CheckLevel


def test_has_percentile(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.ERROR, "check_has_percentile")
    check.has_percentile("ID", 4.5, 0.5)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_has_percentile_with_precision(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.ERROR, "check_has_percentile_with_precision")
    check.has_percentile("ID", 4.5, 0.5, 1000)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


#def test_has_percentile_with_pct(snowpark, configurations):
#    df = snowpark.range(10)
#    check = Check(CheckLevel.ERROR, "check_has_percentile_with_pct")
#    check.has_percentile("ID", 4.5, 0.5, 1000, 0.8)
#    check.config = configurations
#    rs = check.validate(df)
#    assert isinstance(rs, DataFrame)
#    assert rs.first().STATUS == "PASS"
