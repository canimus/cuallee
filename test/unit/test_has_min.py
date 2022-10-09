from cuallee import Check, CheckLevel

def test_lowest_value(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "CheckMin")
    assert check.has_min("id", 0).validate(spark, df).first().status == "PASS"