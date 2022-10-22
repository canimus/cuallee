import numpy as np
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame, Row  # type: ignore
from cuallee import Check, CheckLevel


def test_pos_correlation(snowpark, configurations):
    df = snowpark.range(10).withColumn("id2", F.col("id") * 10)
    check = Check(CheckLevel.WARNING, "check_has_pos_correlation")
    check.has_correlation("ID", "ID2", 1.0)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VALUE == "1.0"


def test_has_no_correlation(snowpark, configurations):
    df = snowpark.createDataFrame(
        [
            Row(
                id1,
                id2,
            )
            for id1, id2 in enumerate([F.randint(0, 100) for f in range(10)])
        ],
        schema=["ID", "ID2"],
    )
    check = Check(CheckLevel.WARNING, "check_has_no_correlation")
    check.has_correlation("ID", "ID2", 1.0)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
