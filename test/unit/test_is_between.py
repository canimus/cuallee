from cuallee import Check, CheckLevel
import inspect


def test_between_method():
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    assert callable(check.is_between)


def test_between_args():
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    signature = inspect.signature(check.is_between)
    params = signature.parameters
    # Expect column and array of values
    assert "column" in params.keys(), "Expected column parameter"
    assert "value" in params.keys(), "Expected value parameter"


def test_between_numbers(spark):
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    assert (
        check.is_between("id", (0, 10)).validate(spark.range(10)).first().status
    )


def test_between_number_with_pct(spark):
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    assert (
        check.is_between("id", (0, 5), pct=0.5)
        .validate(spark.range(10))
        .first()
        .status
    )


def test_between_dates(spark):
    check = Check(CheckLevel.WARNING, "CheckIsBetweenDates")
    df = spark.sql(
        "select explode(sequence(to_date('2022-01-01'), to_date('2022-01-10'), interval 1 day)) as date"
    )
    assert (
        check.is_between("date", ("2022-01-01", "2022-01-10"))
        .validate(df)
        .first()
        .status
    )
