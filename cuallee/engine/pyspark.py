import enum
import importlib
import operator
import os
from dataclasses import dataclass
from functools import partial, reduce
from typing import Any, Callable, Dict, List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Row
from toolz import compose, first, valfilter, valmap
from toolz.curried import map as map_curried

from ..core.check import Check, CheckStatus
from ..core.rule import Rule
from ..engine.base import GenericEngine


class ComputeMethod(enum.Enum):
    OBSERVE = "OBSERVE"
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, List[Column], None]
    expression: Union[Callable[[DataFrame, str], Any], Column]
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


def _replace_observe_compute(computed_expressions: dict) -> dict:
    """Replace observe based check with select"""
    return valmap(
        lambda v: ComputeMethod.SELECT if v == ComputeMethod.OBSERVE else v,
        computed_expressions,
    )


def _compute_observe_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Tuple[int, Dict]:
    """Compute rules throught spark Observation"""

    # Filter expression directed to observe
    _observe = lambda x: x.compute_method.name == ComputeMethod.OBSERVE.name
    observe = valfilter(_observe, compute_set)

    if observe:
        from pyspark.sql import Observation

        observation = Observation("observation")

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in observe.items()
            ],
        )
        rows = df_observation.count()
        # observation_result = observation.get
        return rows, observation.get
    else:
        # observation_result = {}
        rows = dataframe.count()
        return rows, {}


def _compute_select_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark select"""

    # Filter expression directed to select
    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in select.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )


def _compute_transform_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark transform"""

    # Filter expression directed to transform
    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())  # type: ignore
        for k, compute_instruction in transform.items()
    }


class PySparkEngine(GenericEngine):
    """Rule engine for checks in PySpark"""

    def __init__(self):
        pass

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull().cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_empty(self, rule: Rule):
        """Validation for null values in column"""
        predicate = F.col(f"`{rule.column}`").isNull().cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def are_complete(self, rule: Rule):
        """Validation for non-null values in a group of columns"""
        predicate = (
            reduce(
                operator.add,
                [F.col(f"`{c}`").isNotNull().cast("integer") for c in rule.column],
            )
            == len(rule.column)
        ).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.SELECT,
        )

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.count_distinct(F.col(f"`{rule.column}`")),
            ComputeMethod.SELECT,
        )

    def are_unique(self, rule: Rule):
        """Validation for unique values in a group of columns"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.count_distinct(*[F.col(c) for c in rule.column]),
            ComputeMethod.SELECT,
        )

    def is_greater_than(self, rule: Rule):
        """Validation for numeric greater than value"""
        predicate = operator.gt(F.col(f"`{rule.column}`"), rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_greater_or_equal_than(self, rule: Rule):
        """Validation for numeric greater or equal than value"""
        predicate = operator.ge(F.col(f"`{rule.column}`"), rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_less_than(self, rule: Rule):
        """Validation for numeric less than value"""
        predicate = operator.lt(F.col(f"`{rule.column}`"), rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_less_or_equal_than(self, rule: Rule):
        """Validation for numeric less or equal than value"""
        predicate = operator.le(F.col(f"`{rule.column}`"), rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_equal_than(self, rule: Rule):
        """Validation for numeric column equal than value"""
        predicate = operator.eq(F.col(f"`{rule.column}`"), rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def has_pattern(self, rule: Rule):
        """Validation for string type column matching regex expression"""
        predicate = (
            F.length(F.regexp_extract(F.col(f"`{rule.column}`"), f"{rule.value}", 0))
            > 0
        ).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def has_min(self, rule: Rule):
        """Validation of a column's minimum value"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.min(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_max(self, rule: Rule):
        """Validation of a column's maximum value"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.max(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_mean(self, rule: Rule):
        """Validation of a column's average/mean"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_std(self, rule: Rule):
        """Validation of a column's standard deviation"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.stddev_pop(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.SELECT,
        )

    def has_sum(self, rule: Rule):
        """Validation of a column's sum of values"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.sum(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_cardinality(self, rule: Rule):
        """Validation of a column's different values"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.count_distinct(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            ComputeMethod.SELECT,
        )

    def has_infogain(self, rule: Rule):
        """Validation column with more than 1 value"""
        predicate = None
        return ComputeInstruction(
            predicate,
            operator.gt(F.count_distinct(F.col(f"`{rule.column}`")), 1),
            ComputeMethod.SELECT,
        )

    def is_between(self, rule: Rule):
        """Validation of a column between a range"""
        predicate = F.col(f"`{rule.column}`").between(*rule.value).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_contained_in(self, rule: Rule):
        """Validation of column value in set of given values"""
        predicate = F.col(f"`{rule.column}`").isin(list(rule.value)).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def not_contained_in(self, rule: Rule):
        """Validation of column value not in set of given values"""
        predicate = ~F.col(f"`{rule.column}`").isin(list(rule.value))
        return ComputeInstruction(
            predicate,
            F.sum(predicate.cast("long")),
            ComputeMethod.OBSERVE,
        )

    def has_percentile(self, rule: Rule):
        """Validation of a column percentile value"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.percentile_approx(
                F.col(f"`{rule.column}`").cast("double"),
                rule.settings["percentile"],
                rule.settings["precision"],
            ).eqNullSafe(rule.value),
            ComputeMethod.SELECT,
        )

    def is_inside_interquartile_range(self, rule: Rule):
        """Validates a number resides inside the Q3 - Q1 range of values"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return dataframe.select(F.lit(True).alias(key))

        return ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

    def has_min_by(self, rule: Rule):
        """Validation of a column minimum based on other column minimum"""
        column_source, column_target = rule.column
        predicate = None
        return ComputeInstruction(
            predicate,
            F.min_by(column_target, column_source).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_max_by(self, rule: Rule):
        """Validation of a column maximum based on other column maximum"""
        column_source, column_target = rule.column
        predicate = None
        return ComputeInstruction(
            predicate,
            F.max_by(column_target, column_source).eqNullSafe(rule.value),
            ComputeMethod.OBSERVE,
        )

    def has_correlation(self, rule: Rule):
        """Validates the correlation between 2 columns with some tolerance"""
        column_left, column_right = rule.column
        predicate = None
        return ComputeInstruction(
            predicate,
            F.corr(
                F.col(f"`{column_left}`").cast("double"),
                F.col(f"`{column_right}`").cast("double"),
            ).eqNullSafe(F.lit(rule.value)),
            ComputeMethod.SELECT,
        )

    def satisfies(self, rule: Rule):
        """Validation of a column satisfying a SQL-like predicate"""
        predicate = None
        return ComputeInstruction(
            predicate,
            F.sum(F.expr(f"{rule.value}").cast("integer")),
            ComputeMethod.OBSERVE,
        )

    def has_entropy(self, rule: Rule):
        """Validation for entropy calculation on continuous values"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return dataframe.select(F.lit(True).alias(key))

        return ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

    def is_on_weekday(self, rule: Rule):
        """Validates a datetime column is in a Mon-Fri time range"""
        predicate = F.dayofweek(f"`{rule.column}`").between(2, 6).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_weekend(self, rule: Rule):
        """Validates a datetime column is in a Sat-Sun time range"""
        predicate = F.dayofweek(f"`{rule.column}`").isin([1, 7]).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_monday(self, rule: Rule):
        """Validates a datetime column is on Mon"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(2).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_tuesday(self, rule: Rule):
        """Validates a datetime column is on Tue"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(3).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_wednesday(self, rule: Rule):
        """Validates a datetime column is on Wed"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(4).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_thursday(self, rule: Rule):
        """Validates a datetime column is on Thu"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(5).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_friday(self, rule: Rule):
        """Validates a datetime column is on Fri"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(6).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_saturday(self, rule: Rule):
        """Validates a datetime column is on Sat"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(7).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_sunday(self, rule: Rule):
        """Validates a datetime column is on Sun"""
        predicate = F.dayofweek(f"`{rule.column}`").eqNullSafe(1).cast("integer")
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_on_schedule(self, rule: Rule):
        """Validation of a datetime column between an hour interval"""
        predicate = (
            F.hour(F.col(f"`{rule.column}`")).between(*rule.value).cast("integer")
        )
        return ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )

    def is_daily(self, rule: Rule):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return dataframe.select(F.lit(True).alias(key))

        return ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

    def is_custom(self, rule: Rule):
        """Validates dataframe by applying a custom function to the dataframe and resolving boolean values in the last column"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return dataframe.select(F.lit(True).alias(key))

        return ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

    def has_workflow(self, rule: Rule):
        """Validates events in a group clause with order, followed a specific sequence"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return dataframe.select(F.lit(True).alias(key))

        return ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )


def _spark_search(check: Check):
    """Determine if is spark or spark_connect"""
    from pyspark.sql import SparkSession

    # Search for client using spark_connect
    if "SPARK_REMOTE" in os.environ:
        try:
            spark_connect = importlib.import_module("pyspark.sql.connect.session")
            spark = (
                getattr(spark_connect, "SparkSession")
                .builder.remote(os.getenv("SPARK_REMOTE"))
                .getOrCreate()
            )
        except (ModuleNotFoundError, ImportError, AttributeError):
            pass

    elif spark_in_session := valfilter(
        lambda x: isinstance(x, SparkSession), globals()
    ):
        # Obtain the first spark session available in the globals
        spark = first(spark_in_session.values())
    else:
        builder = SparkSession.builder
        # Retrieve config settings from check
        if check.config and isinstance(check.config, dict) and len(check.config.keys()):
            for k, v in check.config.items():
                builder.config(k, v)
        spark = builder.getOrCreate()

    return spark


def _spark_without_observe(spark) -> bool:
    major, minor, _ = map(int, spark.version.split("."))
    with_observe = False
    if not all(map(lambda x: x > 3, [major, minor])) or ("connect" in str(type(spark))):
        with_observe = True

    return with_observe


def validate_data_types(rules, dataframe):
    return True


def summary(check: Check, dataframe: Any):
    spark = _spark_search(check)
    computed_expressions = {
        k: operator.methodcaller(v.method, v)(PySparkEngine())
        for k, v in check._rule.items()
    }

    if _spark_without_observe:
        computed_expressions = _replace_observe_compute(computed_expressions)

    rows, observation_result = _compute_observe_method(computed_expressions, dataframe)
    select_result = _compute_select_method(computed_expressions, dataframe)
    transform_result = _compute_transform_method(computed_expressions, dataframe)
    unified_results = {**observation_result, **select_result, **transform_result}
    check.rows = rows

    for index, (hash_key, rule) in enumerate(check._rule.items(), 1):
        rule.ordinal = index
        rule.evaluate(unified_results[hash_key], rows)

    def _value(x):
        """Removes verbosity for Callable values"""
        if isinstance(x.value, Callable):
            if x.options and isinstance(x.options, dict):
                return x.options.get("custom_value", "f(x)")
        else:
            return str(x.value)

    result = spark.createDataFrame(
        [
            Row(
                rule.ordinal,
                check.date.strftime("%Y-%m-%d %H:%M:%S"),
                check.name,
                check.level.name,
                str(rule.column),
                str(rule.name),
                _value(rule),
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


def ok(check: Check, dataframe: DataFrame) -> bool:
    """True when all rules in the check pass validation"""

    _all_pass = compose(
        all,
        map_curried(partial(operator.eq, CheckStatus.PASS.value)),
        map_curried(operator.attrgetter("status")),
        operator.methodcaller("collect"),
        operator.methodcaller("select", "status"),
    )
    return _all_pass(summary(check, dataframe))


# def _field_type_filter(
#     dataframe: DataFrame,
#     field_type: Union[
#         Type[T.DateType], Type[T.NumericType], Type[T.TimestampType], Type[T.StringType]
#     ],
# ) -> List[str]:
#     """Internal method to search for column names based on data type"""
#     return set(
#         [f.name for f in dataframe.schema.fields if isinstance(f.dataType, field_type)]  # type: ignore
#     )


# def _replace_observe_compute(computed_expressions: dict) -> dict:
#     """Replace observe based check with select"""
#     select_only_expressions = {}
#     for k, v in computed_expressions.items():
#         instruction = v
#         if instruction.compute_method.name == ComputeMethod.OBSERVE.name:
#             instruction.compute_method = ComputeMethod.SELECT
#         select_only_expressions[k] = instruction
#     return select_only_expressions


# def _compute_observe_method(
#     compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
# ) -> Tuple[int, Dict]:
#     """Compute rules throught spark Observation"""

#     # Filter expression directed to observe
#     _observe = lambda x: x.compute_method.name == ComputeMethod.OBSERVE.name
#     observe = valfilter(_observe, compute_set)

#     if observe:
#         from pyspark.sql import Observation

#         observation = Observation("observation")

#         df_observation = dataframe.observe(
#             observation,
#             *[
#                 compute_instruction.expression.alias(hash_key)
#                 for hash_key, compute_instruction in observe.items()
#             ],
#         )
#         rows = df_observation.count()
#         # observation_result = observation.get
#         return rows, observation.get
#     else:
#         # observation_result = {}
#         rows = dataframe.count()
#         return rows, {}


# def _compute_select_method(
#     compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
# ) -> Dict:
#     """Compute rules throught spark select"""

#     # Filter expression directed to select
#     _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
#     select = valfilter(_select, compute_set)

#     return (
#         dataframe.select(
#             *[
#                 compute_instrunction.expression.alias(hash_key)
#                 for hash_key, compute_instrunction in select.items()
#             ]
#         )
#         .first()
#         .asDict()  # type: ignore
#     )


# def _compute_transform_method(
#     compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
# ) -> Dict:
#     """Compute rules throught spark transform"""

#     # Filter expression directed to transform
#     _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
#     transform = valfilter(_transform, compute_set)

#     return {
#         k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())  # type: ignore
#         for k, compute_instruction in transform.items()
#     }


# def numeric_fields(dataframe: DataFrame) -> List[str]:
#     """Filter all numeric data types in data frame and returns field names"""
#     return _field_type_filter(dataframe, T.NumericType)


# def string_fields(dataframe: DataFrame) -> List[str]:
#     """Filter all numeric data types in data frame and returns field names"""
#     return _field_type_filter(dataframe, T.StringType)


# def date_fields(dataframe: DataFrame) -> List[str]:
#     """Filter all date data types in data frame and returns field names"""
#     return set(
#         [f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.DateType) or isinstance(f.dataType, T.TimestampType)]  # type: ignore
#     )


# def timestamp_fields(dataframe: DataFrame) -> List[str]:
#     """Filter all date data types in data frame and returns field names"""
#     return _field_type_filter(dataframe, T.TimestampType)
