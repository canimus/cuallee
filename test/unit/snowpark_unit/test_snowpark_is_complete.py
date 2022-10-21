from cuallee import Check, CheckLevel


def test_is_complete(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "IsComplete")
    check.is_complete("id")
    assert check.validate(df).first().status == "PASS"
