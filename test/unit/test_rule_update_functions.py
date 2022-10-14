from cuallee import Check, CheckLevel


def test_add_rule():
    pass


def test_delete_rule_by_key(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_select_method")
        .is_complete("id")
        .is_unique("id")
    )
    c.validate(df, spark)
    pass


def test_delete_rule_by(spark):
    pass

def test_delete_individual_rule(spark):
    pass



