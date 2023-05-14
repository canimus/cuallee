import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table('bigquery-public-data.chicago_taxi_trips.taxi_trips')
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("taxi_id")
    rs = check.validate(df)
    assert rs.status.str.match('PASS').all()
    assert rs.violations.all() == 0
    assert rs.pass_threshold.all() == 1.0


# def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
#     check.is_complete("id")
#     df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
#     check.table_name = "df"
#     assert check.validate(db).status.str.match("PASS").all()
