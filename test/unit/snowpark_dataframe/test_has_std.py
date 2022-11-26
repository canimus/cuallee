import pytest

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("ID", 2.8722813232690143)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("ID", 2.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("data_type", [[[int(0)], [int(1)], [int(2)], [int(3)], [int(4)], [int(5)], [int(6)], [int(7)], [int(8)], [int(9)]], [[float(0.0)], [float(1.0)], [float(2.0)], [float(3.0)], [float(4.0)], [float(5.0)], [float(6.0)], [float(7.0)], [float(8.0)], [float(9.0)]]], ids=["int", "float"])
def test_parameters(snowpark, data_type):
    df = snowpark.createDataFrame(
        data_type, ["ID"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("ID", 2.8722813232690143)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_std("ID", 2.8722813232690143, 0.7)