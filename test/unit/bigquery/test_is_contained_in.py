import pandas as pd
import pytest
from google.cloud import bigquery

from cuallee.core.check import Check, CheckLevel


def test_positive():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in(
        "payment_type",
        (
            "Way2ride",
            "Prcard",
            "Split",
            "Cash",
            "Credit Card",
            "Unknown",
            "Mobile",
            "No Charge",
            "Dispute",
            "Pcard",
            "Prepaid",
        ),
    )
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_negative():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("payment_type", ("Cash", "Credit Card", "Prepaid"))
    rs = check.validate(df)
    assert rs.status.str.match("FAIL")[1]
    assert rs.violations[1] >= 648794
    assert rs.pass_threshold[1] == 1.0
    assert rs.pass_rate[1] <= 1


@pytest.mark.parametrize(
    "column_name, rule_value",
    [
        [
            "payment_type",
            tuple(
                [
                    "Way2ride",
                    "Prcard",
                    "Split",
                    "Cash",
                    "Credit Card",
                    "Unknown",
                    "Mobile",
                    "No Charge",
                    "Dispute",
                    "Pcard",
                    "Prepaid",
                ]
            ),
        ],
        [
            "payment_type",
            list(
                [
                    "Way2ride",
                    "Prcard",
                    "Split",
                    "Cash",
                    "Credit Card",
                    "Unknown",
                    "Mobile",
                    "No Charge",
                    "Dispute",
                    "Pcard",
                    "Prepaid",
                ]
            ),
        ],
        # [
        #     "pickup_community_area",
        #     [i for i in range(1,78)]
        # ],
        # [
        #     [[1, 10], [2, 15], [3, 17]],
        #     ["id", "test_col"],
        #     (float(10.0), float(15.0), float(17.0)),
        # ],
        # [
        #     [[1, float(10)], [2, float(15)], [3, float(17)]],
        #     ["id", "test_col"],
        #     (10, 15, 17),
        # ],
        # [
        #     [[1, date(2022, 10, 1)], [2, date(2022, 10, 2)], [3, date(2022, 10, 3)]],
        #     ["id", "test_col"],
        #     (date(2022, 10, 1), date(2022, 10, 2), date(2022, 10, 3)),
        # ],
        # [
        #     [
        #         [1, datetime(2022, 10, 1, 10, 0, 0)],
        #         [2, datetime(2022, 10, 1, 11, 0, 0)],
        #         [3, datetime(2022, 10, 1, 12, 0, 0)],
        #     ],
        #     ["id", "test_col"],
        #     (
        #         datetime(2022, 10, 1, 10, 0, 0),
        #         datetime(2022, 10, 1, 11, 0, 0),
        #         datetime(2022, 10, 1, 12, 0, 0),
        #     ),
        # ],
    ],
    ids=(
        "tuple",
        "list",
        # "value_int",
        # "value_float",
        # "data_float",
        # "date",
        # "timestamp",
    ),
)
def test_parameters(column_name, rule_value):
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in(column_name, rule_value)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] == 0
    assert rs.pass_rate[1] == 1.0


def test_coverage():
    df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("payment_type", ("Cash", "Credit Card", "Prepaid"), 0.7)
    rs = check.validate(df)
    assert rs.status.str.match("PASS")[1]
    assert rs.violations[1] >= 648794
    assert rs.pass_threshold[1] == 0.7
    assert rs.pass_rate[1] <= 1
