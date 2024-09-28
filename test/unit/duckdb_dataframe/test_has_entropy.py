import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_entropy("id", 1.0)
    df = pd.DataFrame({"id": [1, 1, 1, 0, 0, 0]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_entropy("id", 1.0)
    df = pd.DataFrame({"id": [10, 10, 10, 10, 50]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values", [[1], [1, 1, 1, 1, 1]], ids=("observation", "classes")
)
def test_parameters(check: Check, db: duckdb.DuckDBPyConnection, values):
    check.has_entropy("id", 0.0)
    df = pd.DataFrame({"id": values})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_entropy("id", 1.0, pct=0.5)
