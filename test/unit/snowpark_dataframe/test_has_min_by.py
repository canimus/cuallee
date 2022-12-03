import pytest
import snowflake.snowpark.functions as F  # type: ignore

from datetime import date, datetime
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.createDataFrame(
        [["Europe", 7073651], ["Asia", 73131839], ["Antartica", 62873]],
        ["CONTINENT", "POPULATION"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min_by("POPULATION", "CONTINENT", "Antartica")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [["Europe", 7073651], ["Asia", 73131839], ["Antartica", 62873]],
        ["CONTINENT", "POPULATION"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min_by("POPULATION", "CONTINENT", "Europe")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize(
    "data, columns, parameter2, parameter3",
    [
        [
            [["Europe", 7073651], ["Asia", 73131839], ["Antartica", 62873]],
            ["CONTINENT", "POPULATION"],
            "CONTINENT",
            "Antartica",
        ],
        [
            [[2012, 7073651], [2013, 73131839], [2014, 62873]],
            ["YEAR", "POPULATION"],
            "YEAR",
            2014,
        ],
        [
            [[0.5, 7073651], [0.6, 73131839], [0.1, 62873]],
            ["PERCENTAGE", "POPULATION"],
            "PERCENTAGE",
            0.1,
        ],
        [
            [
                [date(2012, 1, 11), 7073651],
                [date(2012, 2, 11), 73131839],
                [date(2012, 3, 11), 62873],
            ],
            ["DATE", "POPULATION"],
            "DATE",
            date(2012, 3, 11),
        ],
        [
            [
                [datetime(2012, 1, 11, 10, 10, 10), 7073651],
                [datetime(2012, 1, 11, 10, 11, 10), 73131839],
                [datetime(2012, 1, 11, 0, 10, 10), 62873],
            ],
            ["TIMESTAMP", "POPULATION"],
            "TIMESTAMP",
            datetime(2012, 1, 11, 0, 10, 10),
        ],
    ],
    ids=["string", "int", "float", "date", "timstamp"],
)
def test_parameters(snowpark, data, columns, parameter2, parameter3):
    df = snowpark.createDataFrame(
        data,
        columns,
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min_by("POPULATION", parameter2, parameter3)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_max_by("POPULATION", "CONTINENT", "Antartica", 0.5)
