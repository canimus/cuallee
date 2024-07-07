from cuallee.dagster import make_dagster_checks, make_check_specs, yield_check_results
from cuallee import Check, CheckLevel
import pandas as pd
from typing import Iterator


def test_make_checks():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    check = Check(CheckLevel.WARNING, "Dagster")
    check.is_complete("id")
    result = make_dagster_checks(check, "AssetName", df)
    assert isinstance(result, list)


def test_make_check_specs():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    check = Check(CheckLevel.WARNING, "Dagster")
    check.is_complete("id")
    specs = make_check_specs(check, "test_asset")
    assert isinstance(specs, list)


def test_yield_check_specs():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    check = Check(CheckLevel.WARNING, "Dagster")
    check.is_complete("id")
    results = yield_check_results(check, df)
    assert isinstance(results, Iterator)
