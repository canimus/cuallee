from dagster import asset_check, AssetCheckResult
from cuallee import Check
import pandas as pd
from typing import List


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
