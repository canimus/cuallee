from cuallee import Check, CheckLevel


def test_is_complete(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "IsComplete")
    check.is_complete("ID")
    check.config = configurations
    assert check.validate(df).first().STATUS == "PASS"
