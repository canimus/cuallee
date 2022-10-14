from pyspark.sql import SparkSession, DataFrame, Row, Observation, Column
from typing import Dict, Optional, Callable, Any, Tuple, Union
from toolz import valfilter  # type: ignore
from functools import reduce

import operator
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T

from cuallee import Check, Rule, CheckDataType, ComputeInstruction
from cuallee import dataframe as D


class Compute:
    def __init__(self, name: str):
        self.name = name
        self.compute_instruction = ComputeInstruction

    def __repr__(self):
        return f"Compute(desc:{self.name})"

    # Helper functions:

    def _single_value_rule(
        self,
        column: str,
        value: Optional[Any],
        operator: Callable,
    ):
        return operator(F.col(column), value)

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    # Method functions

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def are_complete(self, rule: Rule):  # To Do with Predicate
        """Validation for non-null values in a group of columns"""
        predicate = [F.col(f"`{c}`").isNotNull() for c in rule.column]
        self.compute_instruction = ComputeInstruction(
            predicate,
            reduce(
                operator.add,
                [
                    F.sum(F.col(f"`{c}`").isNotNull().cast("integer"))
                    for c in rule.column
                ],
            )
            / len(rule.column),
            "observe",
        )
        return self.compute_instruction

    def is_unique(self, rule: Rule):  # To Do with Predicate
        """Validation for unique values in column"""
        predicate = F.col(
            f"`{rule.column}`"
        ).isNotNull()  # F.count_distinct(F.col(rule.column))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(F.col(rule.column)),
            "select",
        )
        return self.compute_instruction

    def are_unique(self, rule: Rule):  # To Do with Predicate
        """Validation for unique values in a group of columns"""
        predicate = F.count_distinct(*[F.col(c) for c in rule.column])
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(*[F.col(c) for c in rule.column]),
            "select",
        )
        return self.compute_instruction

    def is_greater_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric greater than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.gt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def is_greater_or_equal_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric greater or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.ge)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def is_less_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric less than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.lt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def is_less_or_equal_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric less or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.le)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def is_equal_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric column equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.eq)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def has_pattern(self, rule: Rule):  # To Do with Predicate
        """Validation for string type column matching regex expression"""
        predicate = F.length(F.regexp_extract(rule.column, rule.value, 0)) > 0
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            "observe",
        )
        return self.compute_instruction

    def has_min(self, rule: Rule):  # To Do with Predicate
        """Validation of a column’s minimum value"""
        predicate = F.min(F.col(rule.column)).eqNullSafe(rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.min(F.col(rule.column)) == rule.value,
            "observe",
        )
        return self.compute_instruction

    def has_max(self, rule: Rule):  # To Do with Predicate
        """Validation of a column’s maximum value"""
        predicate = F.max(F.col(rule.column)) == rule.value
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.max(F.col(rule.column)) == rule.value,
            "observe",
        )
        return self.compute_instruction

    def has_std(self, rule: Rule):  # To Do with Predicate
        """Validation of a column’s standard deviation"""
        predicate = F.stddev_pop(F.col(rule.column)) == rule.value
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.stddev_pop(F.col(rule.column)) == rule.value,
            "observe",
        )
        return self.compute_instruction

    def has_mean(self, rule: Rule):  # To Do with Predicate
        """Validation of a column's average/mean"""
        predicate = F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.mean(F.col(f"`{rule.column}`")).eqNullSafe(rule.value),
            "observe",
        )
        return self.compute_instruction

    def is_between(self, rule: Rule):  # To Do with Predicate
        """Validation of a column between a range"""
        predicate = F.col(rule.column).between(*rule.value).cast("integer")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate),  # type: ignore
            "observe",
        )
        return self.compute_instruction

    def is_contained_in(self, rule: Rule):  # To Do with Predicate
        """Validation of column value in set of given values"""
        predicate = F.col(rule.column).isin(list(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.LongType())),
            "observe",
        )
        return self.compute_instruction

    def has_percentile(self, rule: Rule):  # To Do with Predicate
        """Validation of a column percentile value"""
        predicate = F.percentile_approx(
            F.col(f"`{rule.column}`").cast(T.DoubleType()), rule.value[1], rule.value[2]
        ).eqNullSafe(rule.value[0])
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.percentile_approx(
                F.col(f"`{rule.column}`").cast(T.DoubleType()),
                rule.value[1],
                rule.value[2],
            ).eqNullSafe(rule.value[0]),
            "select",
        )
        return self.compute_instruction

    def has_max_by(self, rule: Rule):  # To Do with Predicate
        """Validation of a column maximum based on other column maximum"""
        predicate = F.max_by(rule.column[1], rule.column[0]) == rule.value
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.max_by(rule.column[1], rule.column[0]) == rule.value,
            "observe",
        )
        return self.compute_instruction

    def has_min_by(self, rule: Rule):  # To Do with Predicate
        """Validation of a column minimum based on other column minimum"""
        predicate = F.min_by(rule.column[1], rule.column[0]) == rule.value
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.min_by(rule.column[1], rule.column[0]) == rule.value,
            "observe",
        )
        return self.compute_instruction

    def has_correlation(self, rule: Rule):  # To Do with Predicate
        """Validates the correlation between 2 columns with some tolerance"""
        predicate = F.corr(
            F.col(f"`{rule.column[0]}`").cast(T.DoubleType()),
            F.col(f"`{rule.column[1]}`").cast(T.DoubleType()),
        ).eqNullSafe(F.lit(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.corr(
                F.col(f"`{rule.column[0]}`").cast(T.DoubleType()),
                F.col(f"`{rule.column[1]}`").cast(T.DoubleType()),
            ).eqNullSafe(F.lit(rule.value)),
            "select",
        )
        return self.compute_instruction

    def satisfies(self, rule: Rule):  # To Do with Predicate
        """Validation of a column satisfying a SQL-like predicate"""
        predicate = F.expr(rule.value).cast("integer")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(F.expr(rule.value).cast("integer")),
            "observe",
        )
        return self.compute_instruction


def _get_compute_dict(check: Check) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    compute = Compute(check.name)
    for k, v in check._rule.items():
        f = operator.methodcaller(v.method, v)
        check._compute[k] = f(compute)
    return check


def _overwrite_observe_method(check: Check):
    """Overwrite the observe method to select."""
    for v in check._compute.values():
        if v.compute_method == "observe":
            v.compute_method = "select"
        else:
            v.compute_method = v.compute_method


def _get_spark_version(check: Check, spark: SparkSession):
    """Check spark version and overwrite compute method if needed."""
    e = re.compile(r"(\d+).(\d+).(\d+)")
    major, minor, low = list(map(int, e.match(spark.version).groups()))
    if major >= 3 & minor >= 3 & low >= 0:
        pass
    else:
        _overwrite_observe_method(check)


def _column_set_comparison(check: Check, dataframe: DataFrame, columns, filter: Callable, fn: Callable):
    """Compair type of the columns passed in rules and present in dataframe."""
    return set(
        check._compute_columns(
            map(columns, valfilter(filter, check._rule).values())  # type: ignore
        )
    ).difference(fn(dataframe))

import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from typing import Collection, Union, Type


def _field_type_filter(
    dataframe: DataFrame,
    field_type: Union[
        Type[T.DateType], Type[T.NumericType], Type[T.TimestampType], Type[T.StringType]
    ],
) -> Collection:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, field_type)]  # type: ignore
    )


def numeric_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.NumericType)


def string_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.DateType) or isinstance(f.dataType, T.TimestampType) or isinstance(f.dataType, T.TimestampNTZType)]  # type: ignore
    )


def timestamp_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)

def _validate_data_types(check: Check, dataframe: DataFrame):
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""
    _col = operator.attrgetter("column")
    _numeric = lambda x: x.data_type.name == CheckDataType.NUMERIC.name
    _date = lambda x: x.data_type.name == CheckDataType.DATE.name
    _timestamp = lambda x: x.data_type.name == CheckDataType.TIMESTAMP.name
    _string = lambda x: x.data_type.name == CheckDataType.STRING.name
    # Numeric Validation
    non_numeric = _column_set_comparison(check, dataframe, _col, _numeric, numeric_fields)
    assert len(non_numeric) == 0, f"Column(s): {non_numeric} are not numeric"
    # String Validation
    non_string = _column_set_comparison(check, dataframe, _col, _string, string_fields)
    assert len(non_string) == 0, f"Column(s): {non_string} are not strings"
    # Date Validation
    non_date = _column_set_comparison(check, dataframe, _col, _date, date_fields)
    assert len(non_date) == 0, f"Column(s): {non_date} are not dates"
    # Timestamp validation
    non_timestamp = _column_set_comparison(check, dataframe, _col, _timestamp, timestamp_fields)
    assert len(non_timestamp) == 0, f"Column(s): {non_timestamp} are not timestamps"


def _compute_observe_method(check: Check, dataframe: DataFrame) -> Tuple[int, Dict]:
    """Compute rules throught spark Observation"""
    # Filter expression directed to observe
    _observe = lambda x: x.compute_method == "observe"
    observe = valfilter(_observe, check._compute)

    if observe:
        observation = Observation(check.name)

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


def _compute_select_method(check: Check, dataframe: DataFrame) -> Dict:
    """Compute rules throught spark select"""

    # Filter expression directed to select
    _select = lambda x: x.compute_method == "select"
    select = valfilter(_select, check._compute)

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


def _compute_transform_method(check: Check, dataframe: DataFrame) -> Dict:
    """Compute rules throught spark transform"""

    # Filter expression directed to transform
    _transform = lambda x: x.compute_method == "transform"
    transform = valfilter(_transform, check._compute)

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in transform.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )


def compute_summary(
    check: Check, dataframe: DataFrame, spark: SparkSession
) -> DataFrame:
    """Compute all rules in this check for specific data frame"""

    # Compute the expression
    rows, observation_result = _compute_observe_method(check, dataframe)
    select_result = _compute_select_method(check, dataframe)
    transform_result = _compute_transform_method(check, dataframe)

    unified_results = {**observation_result, **select_result, **transform_result}

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "false", F.lit(0.0))
        .when(observed_column == "true", F.lit(1.0))
        .otherwise(observed_column.cast(T.DoubleType()) / rows)  # type: ignore
    )
    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    return (
        spark.createDataFrame(
            [
                Row(  # type: ignore
                    index,
                    rule.method,
                    str(rule.column),
                    str(rule.value),
                    unified_results[hash_key],
                    rule.coverage,
                )
                for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
            ],
            schema="id int, rule string, column string, value string, result string, pass_threshold string",
        )
        .select(
            F.col("id"),
            F.lit(check.date.strftime("%Y-%m-%d")).alias("date"),
            F.lit(check.date.strftime("%H:%M:%S")).alias("time"),
            F.lit(check.name).alias("check"),
            F.lit(check.level.name).alias("level"),
            F.col("column"),
            F.col("rule"),
            F.col("value"),
            F.lit(rows).alias("rows"),
            (rows - F.col("result").cast("long")).alias("violations"),
            _calculate_pass_rate(F.col("result")).alias("pass_rate"),
            F.col("pass_threshold").cast(T.DoubleType()),
        )
        .withColumn(
            "status",
            _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
        )
    )


def _get_rule_status(check: Check, summary_dataframe: DataFrame):
    """Update the rule status after computing summary"""
    for index, rule in enumerate(check._rule.values(), 1):
        rule.status = (
            summary_dataframe.filter(F.col("id") == index)
            .select("status")
            .first()
            .status
        )
    return check


def get_record_sample(
    check: Check,
    dataframe: DataFrame,
    spark: SparkSession,
    status: str = "FAIL",
    method: Union[tuple[str], str] = None,
) -> DataFrame:
    """Give a sample of malformed rows"""

    # Filters
    _sample = (
        lambda x: (x.status == "FAIL")
        if method is None
        else (x.status == "FAIL") & (x.method in method)
    )

    if status == "FAIL":

        sample_dataframe = spark.createDataFrame([], schema=dataframe.schema)

        for hash_key in valfilter(_sample, check._rule).keys():
            sample_dataframe = sample_dataframe.unionByName(
                dataframe.filter(~check._compute[hash_key].predicate)
            )

        return sample_dataframe.distinct()
    else:
        sample_dataframe = dataframe
        for hash_key in valfilter(_sample, check._rule).keys():
            sample_dataframe = sample_dataframe.filter(
                check._compute[hash_key].predicate
            )
        return sample_dataframe
