import logging
import enum
import operator
from dataclasses import dataclass
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window as W
from pyspark.sql import Column, DataFrame, Row
from toolz import first, valfilter  # type: ignore

import cuallee.utils as cuallee_utils
from cuallee import Check, ComputeEngine, Rule
from colorama import Fore, Style  # type: ignore

logger = logging.getLogger(__name__)


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


class Compute(ComputeEngine):
    def __init__(self):
        self.compute_instruction: Union[ComputeInstruction, None] = None

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    def _single_value_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Any],
        operator: Callable,
    ):
        return operator(F.col(f"`{column}`"), value)

    def _stats_fn_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Any],
        operator: Callable,
    ):
        return operator(F.col(column)).eqNullSafe(value)

    # Method functions

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def are_complete(self, rule: Rule):
        """Validation for non-null values in a group of columns"""
        predicate = (
            reduce(
                operator.add,
                [F.col(f"`{c}`").isNotNull().cast("integer") for c in rule.column],
            )
            == len(rule.column)
        ).cast("integer")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None  # F.count_distinct(F.col(rule.column))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(F.col(f"`{rule.column}`")),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def are_unique(self, rule: Rule):
        """Validation for unique values in a group of columns"""
        predicate = None  # TODO:  .groupBy("value").count.filter("count > 1")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(*[F.col(c) for c in rule.column]),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_greater_than(self, rule: Rule):  # To Do with Predicate
        """Validation for numeric greater than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.gt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_greater_or_equal_than(self, rule: Rule):
        """Validation for numeric greater or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.ge)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_less_than(self, rule: Rule):
        """Validation for numeric less than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.lt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_less_or_equal_than(self, rule: Rule):
        """Validation for numeric less or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.le)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_equal_than(self, rule: Rule):
        """Validation for numeric column equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.eq)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_pattern(self, rule: Rule):
        """Validation for string type column matching regex expression"""
        predicate = (
            F.length(F.regexp_extract(F.col(f"`{rule.column}`"), f"{rule.value}", 0))
            > 0
        )
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_min(self, rule: Rule):
        """Validation of a column’s minimum value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.min),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_max(self, rule: Rule):
        """Validation of a column’s maximum value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.max),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_mean(self, rule: Rule):
        """Validation of a column's average/mean"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.mean),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_std(self, rule: Rule):
        """Validation of a column’s standard deviation"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.stddev_pop),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_sum(self, rule: Rule):
        """Validation of a column’s sum of values"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.sum),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_between(self, rule: Rule):
        """Validation of a column between a range"""
        predicate = F.col(f"`{rule.column}`").between(*rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_contained_in(self, rule: Rule):  # To Do with Predicate
        """Validation of column value in set of given values"""
        predicate = F.col(f"`{rule.column}`").isin(list(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.LongType())),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_percentile(self, rule: Rule):
        """Validation of a column percentile value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.percentile_approx(
                F.col(f"`{rule.column}`").cast(T.DoubleType()),
                rule.settings["percentile"],  # type: ignore
                rule.settings["precision"],  # type: ignore
            ).eqNullSafe(
                rule.value  # type: ignore
            ),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_inside_interquartile_range(self, rule: Rule):
        """Validates a number resides inside the Q3 - Q1 range of values"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            _iqr = (
                dataframe.select(
                    F.percentile_approx(
                        f"`{rule.column}`", rule.value  # type: ignore
                    ).alias("iqr")
                )
                .first()
                .iqr
            )
            return dataframe.select(
                F.sum((F.col(f"`{rule.column}`").between(*_iqr)).cast("integer")).alias(
                    key
                )
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

        return self.compute_instruction

    def has_min_by(self, rule: Rule):
        """Validation of a column minimum based on other column minimum"""
        column_source, column_target = rule.column
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.min_by(column_target, column_source) == rule.value,  # type: ignore
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_max_by(self, rule: Rule):
        """Validation of a column maximum based on other column maximum"""
        column_source, column_target = rule.column
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.max_by(column_target, column_source) == rule.value,  # type: ignore
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_correlation(self, rule: Rule):  # To Do with Predicate
        """Validates the correlation between 2 columns with some tolerance"""
        column_left, column_right = rule.column
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.corr(
                F.col(f"`{column_left}`").cast(T.DoubleType()),
                F.col(f"`{column_right}`").cast(T.DoubleType()),
            ).eqNullSafe(F.lit(rule.value)),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def satisfies(self, rule: Rule):  # To Do with Predicate
        """Validation of a column satisfying a SQL-like predicate"""
        predicate = F.expr(f"{rule.value}")
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def has_entropy(self, rule: Rule):
        """Validation for entropy calculation on continuous values"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return (
                dataframe.groupby(F.col(f"`{rule.column}`"))
                .count()
                .select(F.collect_list("count").alias("freq"))
                .select(
                    F.col("freq"),
                    F.aggregate("freq", F.lit(0.0), lambda a, b: a + b).alias("rows"),
                )
                .withColumn("probs", F.transform("freq", lambda x: x / F.col("rows")))
                .withColumn("n_labels", F.size("probs"))
                .withColumn("log_labels", F.log("n_labels"))
                .withColumn("log_prob", F.transform("probs", lambda x: F.log(x)))
                .withColumn(
                    "log_classes",
                    F.transform("probs", lambda x: F.log((x / x) * F.col("n_labels"))),
                )
                .withColumn("entropy_vals", F.arrays_zip("probs", "log_prob"))
                .withColumn(
                    "product_prob",
                    F.transform(
                        "entropy_vals",
                        lambda x: x.getItem("probs") * x.getItem("log_prob"),
                    ),
                )
                .select(
                    (
                        F.aggregate(
                            "product_prob", F.lit(0.0), lambda acc, x: acc + x
                        ).alias("p")
                        / F.col("log_labels")
                        * -1
                    ).alias("entropy")
                )
                .select(
                    F.expr(
                        f"entropy BETWEEN {rule.value-rule.settings['tolerance']} AND {rule.value+rule.settings['tolerance']}"  # type: ignore
                    ).alias(key)
                )
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )
        return self.compute_instruction

    def is_on_weekday(self, rule: Rule):
        """Validates a datetime column is in a Mon-Fri time range"""
        predicate = F.dayofweek(f"`{rule.column}`").between(2, 6)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_weekend(self, rule: Rule):
        """Validates a datetime column is in a Sat-Sun time range"""
        predicate = F.dayofweek(f"`{rule.column}`").isin([1, 7])
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_monday(self, rule: Rule):
        """Validates a datetime column is on Mon"""
        predicate = F.dayofweek(f"`{rule.column}`") == 2
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_tuesday(self, rule: Rule):
        """Validates a datetime column is on Tue"""
        predicate = F.dayofweek(f"`{rule.column}`") == 3
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_wednesday(self, rule: Rule):
        """Validates a datetime column is on Wed"""
        predicate = F.dayofweek(f"`{rule.column}`") == 4
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_thursday(self, rule: Rule):
        """Validates a datetime column is on Thu"""
        predicate = F.dayofweek(f"`{rule.column}`") == 5
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_friday(self, rule: Rule):
        """Validates a datetime column is on Fri"""
        predicate = F.dayofweek(f"`{rule.column}`") == 6
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_saturday(self, rule: Rule):
        """Validates a datetime column is on Sat"""
        predicate = F.dayofweek(f"`{rule.column}`") == 7
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_sunday(self, rule: Rule):
        """Validates a datetime column is on Sun"""
        predicate = F.dayofweek(f"`{rule.column}`") == 1
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_on_schedule(self, rule: Rule):
        """Validation of a datetime column between an hour interval"""
        predicate = F.hour(F.col(f"`{rule.column}`")).between(*rule.value)  # type: ignore
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_daily(self, rule: Rule):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            day_mask = rule.value
            if not day_mask:
                day_mask = [2, 3, 4, 5, 6]

            _weekdays = F.dayofweek(rule.column).isin(*day_mask)  # type: ignore
            _date_only = F.to_date(f"`{rule.column}`").alias(rule.column)  # type: ignore
            full_interval = (
                dataframe.select(
                    F.explode(
                        F.sequence(
                            F.min(F.col(f"`{rule.column}`")),  # type: ignore
                            F.max(F.col(f"`{rule.column}`")),  # type: ignore
                            F.expr("interval 1 day"),
                        )
                    ).alias(
                        f"{rule.column}"
                    )  # type: ignore
                )
                .filter(_weekdays)
                .select(_date_only)
            )
            return full_interval.join(  # type: ignore
                dataframe.select(_date_only), rule.column, how="left_anti"  # type: ignore
            ).select(
                F.when(
                    F.count(f"`{rule.column}`") > 0,
                    (F.count(f"`{rule.column}`") * -1).cast("string"),
                )
                .otherwise("true")
                .alias(key)
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )

        return self.compute_instruction

    def has_workflow(self, rule: Rule):
        """Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            # Where [a] is source node, and [b] destination node
            edges = [F.array(F.lit(a), F.lit(b)) for a, b in rule.value]
            group, event, order = rule.column
            next_event = "CUALLEE_NEXT_EVENT"
            return (
                dataframe.withColumn(
                    next_event, F.lead(event).over(W.partitionBy(group).orderBy(order))
                )
                .withColumn("CUALLEE_EDGE", F.array(F.col(event), F.col(next_event)))
                .select(
                    self._sum_predicate_to_integer(
                        F.col("CUALLEE_EDGE").isin(edges)
                    ).alias(key)
                )
            )

        self.compute_instruction = ComputeInstruction(
            predicate, _execute, ComputeMethod.TRANSFORM
        )

        return self.compute_instruction


def _field_type_filter(
    dataframe: DataFrame,
    field_type: Union[
        Type[T.DateType], Type[T.NumericType], Type[T.TimestampType], Type[T.StringType]
    ],
) -> List[str]:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, field_type)]  # type: ignore
    )


def _replace_observe_compute(computed_expressions: dict) -> dict:
    """Replace observe based check with select"""
    print(
        "[😔]"
        + Fore.YELLOW
        + " PySpark < 3.3.0 | When you upgrade checks will run 2x faster."
    )
    print(Style.RESET_ALL)
    select_only_expressions = {}
    for k, v in computed_expressions.items():
        instruction = v
        if instruction.compute_method.name == ComputeMethod.OBSERVE.name:
            instruction.compute_method = ComputeMethod.SELECT
        select_only_expressions[k] = instruction
    return select_only_expressions


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


def numeric_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.NumericType)


def string_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.DateType) or isinstance(f.dataType, T.TimestampType)]  # type: ignore
    )


def timestamp_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def validate_data_types(rules: List[Rule], dataframe: DataFrame) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""

    # COLUMNS
    # =======
    rule_match = cuallee_utils.match_columns(
        rules, dataframe.columns, case_sensitive=False
    )
    assert not rule_match, f"Column(s): {rule_match} are not present in dataframe"

    # NUMERIC
    # =======
    numeric_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_numeric_rules(rules)
    )
    numeric_dtypes = numeric_fields(dataframe)
    numeric_match = cuallee_utils.match_data_types(numeric_columns, numeric_dtypes)
    assert not numeric_match, f"Column(s): {numeric_match} are not numeric"

    # DATE
    # =======
    date_columns = cuallee_utils.get_rule_columns(cuallee_utils.get_date_rules(rules))
    date_dtypes = date_fields(dataframe)
    date_match = cuallee_utils.match_data_types(date_columns, date_dtypes)
    assert not date_match, f"Column(s): {date_match} are not date"

    # TIMESTAMP
    # =======
    timestamp_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_timestamp_rules(rules)
    )
    timestamp_dtypes = timestamp_fields(dataframe)
    timestamp_match = cuallee_utils.match_data_types(
        timestamp_columns, timestamp_dtypes
    )
    assert not timestamp_match, f"Column(s): {timestamp_match} are not timestamp"

    # STRING
    # =======
    string_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_string_rules(rules)
    )
    string_dtypes = string_fields(dataframe)
    string_match = cuallee_utils.match_data_types(string_columns, string_dtypes)
    assert not string_match, f"Column(s): {string_match} are not string"

    return True


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def summary(check: Check, dataframe: DataFrame) -> DataFrame:
    """Compute all rules in this check for specific data frame"""
    from pyspark.sql.session import SparkSession

    # Check SparkSession is available in environment through globals
    if spark_in_session := valfilter(lambda x: isinstance(x, SparkSession), globals()):
        # Obtain the first spark session available in the globals
        spark = first(spark_in_session.values())
    else:
        # TODO: Check should have options for compute engine
        spark = SparkSession.builder.getOrCreate()

    # Compute the expression
    computed_expressions = compute(check._rule)
    if int(spark.version.replace(".", "")[:3]) < 330:
        computed_expressions = _replace_observe_compute(computed_expressions)

    rows, observation_result = _compute_observe_method(computed_expressions, dataframe)
    select_result = _compute_select_method(computed_expressions, dataframe)
    transform_result = _compute_transform_method(computed_expressions, dataframe)

    unified_results = {**observation_result, **select_result, **transform_result}
    logger.debug(unified_results)

    _calculate_violations = lambda result_column: (
        F.when(result_column < 0, F.abs(result_column))
        .when(result_column == "false", F.lit(rows))
        .when(result_column == "true", F.lit(0))
        .otherwise(rows - result_column.cast("long"))
    )

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "false", F.lit(0.0))
        .when(observed_column == "true", F.lit(1.0))
        .when(
            (observed_column < 0) & (F.abs(observed_column) > F.lit(rows)),
            rows / F.abs(observed_column),
        )  # Calculate as ratio or rows
        .when(
            (observed_column < 0) & (F.abs(observed_column) < F.lit(rows)),
            1 - (F.abs(observed_column) / rows),
        )  # Calculate as total
        .when(
            (observed_column < 0) & (F.abs(observed_column) == F.lit(rows)), F.lit(0.5)
        )  # Half in, half not
        .otherwise(observed_column.cast(T.DoubleType()) / rows)  # type: ignore
    )

    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    result = (
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
            F.lit(check.date.strftime("%Y-%m-%d %H:%M:%S")).alias("timestamp"),
            F.lit(check.name).alias("check"),
            F.lit(check.level.name).alias("level"),
            F.col("column"),
            F.col("rule"),
            F.col("value"),
            F.lit(rows).alias("rows"),
            _calculate_violations(F.col("result")).alias("violations"),
            _calculate_pass_rate(F.col("result")).alias("pass_rate"),
            F.col("pass_threshold").cast(T.DoubleType()),
        )
        .withColumn(
            "status",
            _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
        )
    )

    logger.debug(result.collect())
    return result


# def _get_rule_status(check: Check, summary_dataframe: DataFrame):
#     """Update the rule status after computing summary"""
#     for index, rule in enumerate(check._rule.values(), 1):
#         rule.status = (
#             summary_dataframe.filter(F.col("id") == index)
#             .select("status")
#             .first()
#             .status
#         )
#     return check


# def get_record_sample(
#     check: Check,
#     dataframe: DataFrame,
#     spark: SparkSession,
#     status: str = "FAIL",
#     method: Union[tuple[str], str] = None,
# ) -> DataFrame:
#     """Give a sample of malformed rows"""

#     # Filters
#     _sample = (
#         lambda x: (x.status == "FAIL")
#         if method is None
#         else (x.status == "FAIL") & (x.method in method)
#     )

#     if status == "FAIL":

#         sample_dataframe = spark.createDataFrame([], schema=dataframe.schema)

#         for hash_key in valfilter(_sample, check._rule).keys():
#             sample_dataframe = sample_dataframe.unionByName(
#                 dataframe.filter(~check._compute[hash_key].predicate)
#             )

#         return sample_dataframe.distinct()
#     else:
#         sample_dataframe = dataframe
#         for hash_key in valfilter(_sample, check._rule).keys():
#             sample_dataframe = sample_dataframe.filter(
#                 check._compute[hash_key].predicate
#             )
#         return sample_dataframe
