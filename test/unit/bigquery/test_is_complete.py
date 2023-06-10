import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table('bigquery-public-data.chicago_taxi_trips.taxi_trips')
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("taxi_id")
    rs = check.validate(df)
    assert rs.status.str.match('PASS')[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table('bigquery-public-data.chicago_taxi_trips.taxi_trips')
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("trip_end_timestamp")
    rs = check.validate(df)
    assert rs.status.str.match('FAIL')[1]
    assert rs.violations[1] == 18418
    assert rs.pass_rate[1] == 207158238/207176656


def test_parameters():
    return "😅 No parameters to be tested!"


def test_coverage():
    df = bigquery.dataset.Table('bigquery-public-data.chicago_taxi_trips.taxi_trips')
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("trip_end_timestamp", 0.7)
    rs = check.validate(df)
    assert rs.status.str.match('PASS')[1]
    assert rs.violations[1] == 18418
    assert rs.pass_threshold[1] == 0.7
    assert rs.pass_rate[1] == 207158238/207176656