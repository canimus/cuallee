import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("id", 2.8722813232690143)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("id", 2.5)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "data_type",
    [
        [
            [int(0)],
            [int(1)],
            [int(2)],
            [int(3)],
            [int(4)],
            [int(5)],
            [int(6)],
            [int(7)],
            [int(8)],
            [int(9)],
        ],
        [
            [float(0.0)],
            [float(1.0)],
            [float(2.0)],
            [float(3.0)],
            [float(4.0)],
            [float(5.0)],
            [float(6.0)],
            [float(7.0)],
            [float(8.0)],
            [float(9.0)],
        ],
    ],
    ids=["int", "float"],
)
def test_parameters(spark, data_type):
    df = spark.createDataFrame(data_type, ["id"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_std("id", 2.8722813232690143)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_std("id", 2.8722813232690143, 0.7)
