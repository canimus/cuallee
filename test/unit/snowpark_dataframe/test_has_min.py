from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_stats_min_value(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_has_min")
    check.has_min("ID", 0.0)
    check.config = configurations
<<<<<<< HEAD:test/unit/snowpark_dataframe/test_has_min.py
    assert check.validate(df).first().STATUS == "PASS"
=======
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_stats_min_value_int(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_has_min_int")
    check.has_min("ID", 0)
    check.config = configurations
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"
>>>>>>> main:test/unit/snowflake/test_has_min.py
