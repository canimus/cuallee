import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_has_max_by_object(snowpark, configurations):
    df = snowpark.createDataFrame(
        [["Europe", 7073651], ["Asia", 73131839], ["Antartica", 62873]],
        ["CONTINENT", "POPULATION"],
    )
    check = Check(CheckLevel.WARNING, "check_has_max_by_object")
    check.has_max_by("POPULATION", "CONTINENT", "Asia")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_has_max_by_numeric(snowpark, configurations):
    df = snowpark.createDataFrame(
        [[2012, 7073651], [2013, 73131839], [2014, 62873]], ["YEAR", "POPULATION"]
    )
    check = Check(CheckLevel.WARNING, "check_has_max_by_numeric")
    check.has_max_by("POPULATION", "YEAR", 2013)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
