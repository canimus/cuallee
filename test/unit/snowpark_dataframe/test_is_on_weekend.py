import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(2).withColumn(
        "ARRIVAL_DATE", F.date_from_parts(2022, 11, F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0
    

def test_negative(snowpark):
    df = snowpark.range(5).withColumn(
        "ARRIVAL_DATE", F.date_from_parts(2022, 11, F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 3
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 2/5
    

def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.range(9).withColumn(
        "ARRIVAL_DATE", F.date_from_parts(2022, 11, F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE", 0.4)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 5
    assert rs.first().PASS_THRESHOLD == 0.4
    assert rs.first().PASS_RATE == 4/9
