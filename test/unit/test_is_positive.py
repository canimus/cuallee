from cuallee import Check, CheckLevel

def test_natural_numbers(spark):
    df = spark.range(1,10)
    check = Check(CheckLevel.WARNING, "Zero Failure")
    check.is_positive("id")
    assert check.validate(df).first().status == "PASS"

def test_zero(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "Zero Failure")
    check.is_positive("id")
    assert check.validate(df).first().status == "FAIL"
