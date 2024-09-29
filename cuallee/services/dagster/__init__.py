from dagster import asset_check, AssetCheckResult, AssetCheckSpec, AssetCheckSeverity
from cuallee import Check, CheckLevel
import pandas as pd
from typing import List, Iterator


def make_dagster_checks(
    check: Check, asset: str, data: pd.DataFrame
) -> List[AssetCheckResult]:
    results = check.validate(data)
    checks = []
    for item in results.itertuples():

        @asset_check(name=f"{item.rule}", asset=asset)
        def _check():
            return AssetCheckResult(
                passed=(item.status == "PASS"),
                metadata={
                    "level": item.level,
                    "rows": int(item.rows),
                    "column": item.column,
                    "value": str(item.value),
                    "violations": int(item.violations),
                    "pass_rate": item.pass_rate,
                },
            )

        checks.append(_check)
    return checks


def get_severity(check: Check):
    if check.level == CheckLevel.ERROR:
        return AssetCheckSeverity.ERROR
    else:
        return AssetCheckSeverity.WARN


def make_check_specs(check: Check, asset_name: str) -> List[AssetCheckSpec]:
    """To be used in the @asset decorator as input for check_specs"""
    return [
        AssetCheckSpec(name=f"{rule.method}.{rule.column}", asset=asset_name)
        for rule in check.rules
    ]


def yield_check_results(
    check: Check, dataframe: pd.DataFrame
) -> Iterator[AssetCheckResult]:
    """Used in checks inside an asset, to yield all cuallee validations"""
    results = check.validate(dataframe)

    for item in results.itertuples():
        yield AssetCheckResult(
            check_name=f"{item.rule}.{item.column}",
            passed=(item.status == "PASS"),
            metadata={
                "level": item.level,
                "rows": int(item.rows),
                "column": item.column,
                "value": str(item.value),
                "violations": int(item.violations),
                "pass_rate": item.pass_rate,
            },
            severity=get_severity(check),
        )
