import pytest

from google.cloud import bigquery

from cuallee import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("unique_key", "taxi_id"))
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("taxi_id", "pickup_community_area"))
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]
    assert rs.violations[1] == 208532125
    assert rs.pass_threshold[1] == 1.0
    assert rs.pass_rate[1] == 411496 / 208943621


@pytest.mark.parametrize(
    "rule_column",
    [tuple(["unique_key", "taxi_id"]), list(["unique_key", "taxi_id"])],
    ids=("tuple", "list"),
)
def test_parameters(rule_column):
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(rule_column)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("taxi_id", "pickup_community_area"), 0.001)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 208532125
    assert rs.pass_threshold[1] == 0.001
    assert rs.pass_rate[1] == 411496 / 208943621
