import pytest
import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("taxi_id", "unique_key"))
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("trip_start_timestamp", "trip_end_timestamp"))
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]
    assert rs.violations[1] >= 9217
    assert rs.pass_threshold[1] == 1.0
    assert rs.pass_rate[1] >= 0.99


@pytest.mark.parametrize(
    "rule_column",
    [tuple(["taxi_id", "unique_key"]), list(["taxi_id", "unique_key"])],
    ids=("tuple", "list"),
)
def test_parameters(spark, rule_column):
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(rule_column)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("trip_start_timestamp", "trip_end_timestamp"), 0.7)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] >= 9217
    assert rs.pass_threshold[1] == 0.7
    assert rs.pass_rate[1] >= 0.99
