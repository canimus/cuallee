import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame, Row  # type: ignore
from cuallee import Check, CheckLevel


def test_regex_pattern(snowpark, configurations):
    df = snowpark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["uno", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.ERROR, "check_has_pattern")
    check.has_pattern("NAME", r".*s$")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 1, "Uno should violate the expression"
