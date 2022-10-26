from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_has_entropy(snowpark, configurations):
    df = snowpark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "check_has_entropy")
    check.has_entropy("TEST", 1.0, 0.1)
    check.config = configurations
    rs = check.validate(df)
    assert (rs, DataFrame)
    assert rs.first().STATUS == "PASS"
