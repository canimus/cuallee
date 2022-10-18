from cuallee import Check, CheckLevel

def test_natural_numbers(spark):
    df = spark.range(1e6,1e7)
    check = Check(CheckLevel.WARNING, "Zero Failure")
    check.is_in_millions("id")
    assert check.validate(df).first().status == "PASS"

