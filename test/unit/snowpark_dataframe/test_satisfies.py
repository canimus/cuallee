import pytest
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark.exceptions import SnowparkSQLException
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("ID", "((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("ID", "((id BETWEEN 0 and 9) AND (id > 0) AND (id <= 10))")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize(
    "columns, rule_value",
    [
        ["ID", "((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))"],
        [tuple(["ID", "ID2"]), "((id * id2) > 10) OR ((id * id2) = 0)"],
        [list(["ID", "ID2"]), "((id * id2) > 10) OR ((id * id2) = 0)"],
    ],
    ids=("one_column", "two_columns_tuple", "two_columns_list"),
)
def test_parameters(snowpark, columns, rule_value):
    df = snowpark.range(10).withColumn("id2", (F.col("id") + 1) * 100)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies(columns, rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("ID", "((id BETWEEN 0 and 9) AND (id > 0) AND (id <= 10))", 0.9)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 1
    assert rs.first().PASS_THRESHOLD == 0.9
    assert rs.first().PASS_RATE == 0.9


def test_col_name_error(snowpark):
    df = snowpark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.WARNING, "check_predicate_on_unknown_columns")
    check.satisfies(["ID", "ID2"], "(id * id3) > 10", 0.9)
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        check.validate(df)
