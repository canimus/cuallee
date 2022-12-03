import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn(
        "date", F.date_from_parts(2022, 11, 29 + F.col("ID") * 7)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_tuesday("DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.range(10).withColumn(
        "date", F.date_from_parts(2022, 11, 29 + F.col("ID"))
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_tuesday("DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 8
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 0.2


def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.range(10).withColumn(
        "date", F.date_from_parts(2022, 11, 29 + F.col("ID"))
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_tuesday("DATE", 0.2)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 8
    assert rs.first().PASS_THRESHOLD == 0.2
    assert rs.first().PASS_RATE == 0.2
