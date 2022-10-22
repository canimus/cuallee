from cuallee import Check, CheckLevel


def test_natural_numbers(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "SnowParkHasMin")
    check.has_min("ID", 0.0)
    check.config = configurations
    assert check.validate(df).first().STATUS == "PASS"
