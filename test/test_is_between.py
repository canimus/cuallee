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
    assert "column" in params.keys()
    assert "value" in params.keys()

def test_between_numbers(spark):
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    check.is_between("id", 0, 10)