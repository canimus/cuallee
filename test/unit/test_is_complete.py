from cuallee import Check, CheckLevel


def test_single_column(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "IsComplete")
    check.is_complete("id")
    assert check.validate(spark, df).first().status == "PASS"
