import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("taxi_id")
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("trip_end_timestamp")
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]
    assert rs.violations[1] >= 1589
    assert rs.pass_threshold[1] == 1.0
    


# def test_parameters():
#     return "😅 No parameters to be tested!"


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("trip_end_timestamp", 0.7)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] >= 1589
    #assert rs.pass_threshold[1] == 0.7
    # assert rs.pass_rate[1] == 0.9999117752439066  # 207158222/207176656
