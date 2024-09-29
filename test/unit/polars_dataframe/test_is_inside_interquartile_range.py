import polars as pl

from cuallee import Check


def test_positive(check: Check):
    df = pl.DataFrame(
        {
            "id": [
                12,
                13,
                15,
                18,
                19,
                22,
                88,
                89,
                90,
                91,
                92,
                93,
                95,
                98,
                99,
                101,
                101,
                103,
                105,
                106,
                107,
                108,
                109,
                200,
                201,
                201,
                203,
                204,
                215,
                216,
                217,
                222,
                223,
                224,
                225,
                227,
                229,
                230,
                232,
                245,
                246,
                250,
                258,
                270,
                271,
                271,
                272,
                273,
            ]
        }
    )
    check.is_inside_interquartile_range("id", pct=0.5)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    df = pl.DataFrame(
        {
            "id": [
                12,
                13,
                15,
                18,
                19,
                22,
                88,
                89,
                90,
                91,
                92,
                93,
                95,
                98,
                99,
                101,
                101,
                103,
                105,
                106,
                107,
                108,
                109,
                200,
                201,
                201,
                203,
                204,
                215,
                216,
                217,
                222,
                223,
                224,
                225,
                227,
                229,
                230,
                232,
                245,
                246,
                250,
                258,
                270,
                271,
                271,
                272,
                273,
            ]
        }
    )
    check.is_inside_interquartile_range("id")
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    df = pl.DataFrame(
        {
            "id": [
                12,
                13,
                15,
                18,
                19,
                22,
                88,
                89,
                90,
                91,
                92,
                93,
                95,
                98,
                99,
                101,
                101,
                103,
                105,
                106,
                107,
                108,
                109,
                200,
                201,
                201,
                203,
                204,
                215,
                216,
                217,
                222,
                223,
                224,
                225,
                227,
                229,
                230,
                232,
                245,
                246,
                250,
                258,
                270,
                271,
                271,
                272,
                273,
            ]
        }
    )
    check.is_inside_interquartile_range("id", pct=0.5)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
