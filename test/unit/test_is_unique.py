from cuallee import Check, CheckLevel


def test_unique_column(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "UniqueColumn")
    check.is_unique("id")
    assert check.validate(spark, df).first().status == "PASS"
