import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("unique_key")
    rs = check.validate(df)
    # assert rs.status.str.match("PASS")[1]
    # assert rs.violations[1] == 0
    #assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("taxi_id")
    rs = check.validate(df)
    #assert rs.status.str.match("FAIL")[1]
    #assert rs.violations[1] >= 102580503
    #assert rs.pass_threshold[1] == 1.0
    # assert rs.pass_rate[1] == 9738 / 208943621


# def test_parameters():
#     return "😅 No parameters to be tested!"


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("taxi_id", 0.000007)
    rs = check.validate(df)
    #assert rs.status.str.match("PASS")[1]
    #assert rs.violations[1] >= 102580503
    # assert rs.pass_threshold[1] == 0.000007
    # assert rs.pass_rate[1] == 9738 / 208943621
