import duckdb
import pandas as pd

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    df = pd.DataFrame(
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
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.eq("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    df = pd.DataFrame(
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
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.eq("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    df = pd.DataFrame(
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
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
