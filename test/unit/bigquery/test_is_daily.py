import pytest
import pandas as pd

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("trip_start_timestamp")
    rs = check.validate(df)
    # assert rs.violations[1] > 1


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("trip_end_timestamp")
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]
    # assert rs.violations[1] >= 1
    # assert rs.pass_rate[1] <= 208914146 / 208943621


@pytest.mark.parametrize(
    "rule_value",
    [list([2, 3, 4, 5, 6]), list([1, 3, 5]), tuple([1, 3, 5])],
    ids=("default", "three_days_list", "three_days_tuple"),
)
def test_parameters(rule_value):
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("trip_start_timestamp", rule_value)
    rs = check.validate(df)
    # assert rs.status.str.match("FAIL")[1]
    # assert rs.violations[1] > 0


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("trip_end_timestamp", pct=0.7)
    rs = check.validate(df)
    # assert rs.status.str.match("PASS")[1]
    # assert rs.pass_threshold[1] == 0.7
    # assert rs.pass_rate[1] <= 208914146 / 208943621
