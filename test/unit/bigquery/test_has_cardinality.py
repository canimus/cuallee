import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_cardinality("payment_type", 11)
    rs = check.validate(df)

    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_cardinality("payment_type", 1)
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]


# def test_parameters():
#     return "😅 No parameters to be tested!"
