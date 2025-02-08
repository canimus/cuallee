import operator
from functools import partial
from typing import Any, List, Union

from pyspark.sql import DataFrame, Row
from toolz import compose
from toolz.curried import map as map_curried

from ...core.check import Check, CheckStatus
from ...core.rule import Rule
from . import predicates as P
from . import utils as U


def dtypes(rules: List[Rule], dataframe: Union[DataFrame, Any]):
    """Validates affinity on data types prior to validation"""
    return True


def summary(check: Check, dataframe: Union[DataFrame, Any]):
    spark = U.find_spark(check.config)
    computed_expressions = {
        k: operator.methodcaller(v.method, v)(P) for k, v in check._rule.items()
    }

    if U.has_observe(spark):
        computed_expressions = U.make_backwards_compatible(computed_expressions)

    rows, observation_result = U.compute_observations(computed_expressions, dataframe)
    select_result = U.compute_selections(computed_expressions, dataframe)
    transform_result = U.compute_transformations(computed_expressions, dataframe)
    unified_results = {**observation_result, **select_result, **transform_result}
    check.rows = rows

    for index, (hash_key, rule) in enumerate(check._rule.items(), 1):
        rule.ordinal = index
        rule.evaluate(unified_results[hash_key], rows)

    result = spark.createDataFrame(
        [
            Row(
                rule.ordinal,
                check.date.strftime("%Y-%m-%d %H:%M:%S"),
                check.name,
                check.level.name,
                str(rule.column),
                str(rule.name),
                str(rule.format_value()),
                int(check.rows),
                int(rule.violations),
                float(rule.pass_rate),
                float(rule.coverage),
                rule.status,
            )
            for rule in check.rules
        ],
        schema="id int, timestamp string, check string, level string, column string, rule string, value string, rows bigint, violations bigint, pass_rate double, pass_threshold double, status string",
    )

    return result


def ok(check: Check, dataframe: Union[DataFrame, Any]) -> bool:
    """True when all rules in the check pass validation"""

    _all_pass = compose(
        all,
        map_curried(partial(operator.eq, CheckStatus.PASS.value)),
        map_curried(operator.attrgetter("status")),
        operator.methodcaller("collect"),
        operator.methodcaller("select", "status"),
    )
    return _all_pass(summary(check, dataframe))
