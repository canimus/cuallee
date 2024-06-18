import enum
import os
import operator
import snowflake.snowpark.functions as F  # type: ignore
import snowflake.snowpark.types as T  # type: ignore
import snowflake.snowpark.window as W  # type: ignore

from typing import (
    Union,
    Dict,
    Type,
    Callable,
    Iterable,
    Optional,
    Any,
    Tuple,
    List,
)
from dataclasses import dataclass
from snowflake.snowpark import DataFrame, Column, Session, Row
from snowflake.snowpark.session import Session as SnowSession
from toolz import valfilter, first  # type: ignore
from functools import reduce

from cuallee import Check, Rule
import cuallee.utils as cuallee_utils


class ComputeMethod(enum.Enum):
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, None]
    expression: Column
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Compute:
    def __init__(self):
        self.compute_instruction = None

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    def _single_value_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Union[Tuple[Any], Iterable[Any], Any]],
        operator: Callable,
    ):
        return operator(F.col(column), value)

    def _stats_fn_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Any],
        operator: Callable,
    ):
        return operator(F.col(column)).eqNullSafe(value)

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(rule.column).isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_empty(self, rule: Rule):
        """Validation for null values in column"""
        predicate = F.col(rule.column).isNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def are_complete(self, rule: Rule):
        """Validation for non-null values in a group of columns"""
        predicate = [F.col(c).isNotNull() for c in rule.column]
        self.compute_instruction = ComputeInstruction(
            predicate,
            reduce(
                operator.add,
                [self._sum_predicate_to_integer(p) for p in predicate],
            )
            / len(rule.column),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None  # TODO:  .groupBy("value").count.filter("count > 1")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(F.col(rule.column)),
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

    def is_greater_than(self, rule: Rule):
        """Validation for numeric greater than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.gt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_greater_or_equal_than(self, rule: Rule):
        """Validation for numeric greater or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.ge)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_less_than(self, rule: Rule):
        """Validation for numeric less than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.lt)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_less_or_equal_than(self, rule: Rule):
        """Validation for numeric less or equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.le)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_equal_than(self, rule: Rule):
        """Validation for numeric column equal than value"""
        predicate = self._single_value_rule(rule.column, rule.value, operator.eq)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_pattern(self, rule: Rule):
        """Validation for string type column matching regex expression"""
        predicate = F.regexp_count(rule.column, rule.value, 1) > 0
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_min(self, rule: Rule):
        """Validation of a column’s minimum value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.min),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_max(self, rule: Rule):
        """Validation of a column’s maximum value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.max),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_mean(self, rule: Rule):
        """Validation of a column's average/mean"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.mean),
            ComputeMethod.SELECT,
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
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_cardinality(self, rule: Rule):
        """Validation of a column’s distinct values"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._stats_fn_rule(rule.column, rule.value, F.count_distinct),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_infogain(self, rule: Rule):
        """More than 1 different value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            operator.gt(F.count_distinct(F.col(f"`{rule.column}`")), 1),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_between(self, rule: Rule):
        """Validation of a column between a range"""
        predicate = F.col(rule.column).between(*rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_contained_in(self, rule: Rule):
        """Validation of column value in set of given values"""
        predicate = F.col(rule.column).isin(list(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.LongType())),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def not_contained_in(self, rule: Rule):
        """
        Validates that each value in the specified column does not exist in the provided set of values.
        """
        predicate = ~F.col(rule.column).isin(list(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.IntegerType())),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_percentile(self, rule: Rule):
        """Validation of a column percentile value"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.approx_percentile(
                F.col(rule.column).cast(T.DoubleType()),
                rule.settings["percentile"],  # type: ignore
            ).eqNullSafe(
                rule.value  # type: ignore
            ),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_inside_interquartile_range(self, rule: Rule):
        """Verifies values inside the IQR of a vector"""
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            _iqr = dataframe.select(
                [F.approx_percentile(rule.column, value) for value in rule.value]  # type: ignore
            ).first()
            return dataframe.select(
                self._sum_predicate_to_integer(
                    F.col(rule.column).between(_iqr[0], _iqr[1])
                ).alias(key)
            )

        self.compute_instruction = ComputeInstruction(
            predicate, _execute, ComputeMethod.TRANSFORM
        )

        return self.compute_instruction

    def has_min_by(self, rule: Rule):
        """Validation of a column value based on another column minimum"""
        column_source, column_target = rule.column
        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return (
                dataframe.filter(
                    F.col(column_source)
                    == dataframe.select(F.min(column_source).alias("MIN")).first().MIN
                )
                .filter(F.col(column_target) == rule.value)
                .select(F.count(column_target).cast("boolean").alias(key))
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )
        return self.compute_instruction

    def has_max_by(self, rule: Rule):
        """Validation of a column value based on another column maximum"""
        column_source, column_target = rule.column

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            return (
                dataframe.filter(
                    F.col(column_source)
                    == dataframe.select(F.max(column_source).alias("MAX")).first().MAX
                )
                .filter(F.col(column_target) == rule.value)
                .select(F.count(column_target).cast("boolean").alias(key))
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )
        return self.compute_instruction

    def has_correlation(self, rule: Rule):
        """Validates the correlation between 2 columns with some tolerance"""
        column_left, column_right = rule.column
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.corr(
                F.col(column_left).cast(T.DoubleType()),
                F.col(column_right).cast(T.DoubleType()),
            ).eqNullSafe(F.lit(rule.value)),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def satisfies(self, rule: Rule):
        """Validation of a column satisfying a SQL-like predicate"""
        predicate = F.sql_expr(rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def has_entropy(self, rule: Rule):
        """Validation for entropy calculation on continuous values"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            rows = dataframe.count()
            _split_to_table = F.table_function("split_to_table")
            p = (
                dataframe.groupBy(rule.column)
                .count()
                .select(F.array_agg("count").alias("FREQ"))
                .join_table_function(
                    _split_to_table(
                        F.array_to_string(F.col("FREQ"), F.lit(" ")), F.lit(" ")
                    ).alias("CUALLEE_SEQ", "CUALLEE_IDX", "CUALLEE_VALUE")
                )
                .withColumn("probs", F.div0(F.col("CUALLEE_VALUE"), rows))
                .withColumn("n_labels", F.array_size("FREQ"))
                .withColumn("log_labels", F.log(2, "n_labels"))
                .withColumn("log_prob", F.log(2, F.col("probs")))
                .withColumn("product_prob", F.col("probs") * F.col("log_prob"))
            )
            return p.select(
                (
                    F.div0(
                        F.sum(F.col("product_prob")),
                        p.select(F.col("log_labels").alias("LOG_LABELS"))
                        .first()
                        .LOG_LABELS,
                    )
                    * -1
                ).alias("entropy")
            ).select(
                F.sql_expr(
                    f"entropy BETWEEN {rule.value-rule.settings['tolerance']} AND {rule.value+rule.settings['tolerance']}"
                ).alias(key)
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )
        return self.compute_instruction

    def is_on_weekday(self, rule: Rule):
        """Validates a datetime column is in a Mon-Fri time range"""
        predicate = F.dayofweek(rule.column).between(1, 5)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_weekend(self, rule: Rule):
        """Validates a datetime column is in a Sat-Sun time range"""
        predicate = F.dayofweek(rule.column).isin([0, 6])
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_monday(self, rule: Rule):
        """Validates a datetime column is on Mon"""
        predicate = F.dayofweek(rule.column) == 1
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_tuesday(self, rule: Rule):
        """Validates a datetime column is on Tue"""
        predicate = F.dayofweek(rule.column) == 2
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_wednesday(self, rule: Rule):
        """Validates a datetime column is on Wed"""
        predicate = F.dayofweek(rule.column) == 3
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_thursday(self, rule: Rule):
        """Validates a datetime column is on Thu"""
        predicate = F.dayofweek(rule.column) == 4
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_friday(self, rule: Rule):
        """Validates a datetime column is on Fri"""
        predicate = F.dayofweek(rule.column) == 5
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_saturday(self, rule: Rule):
        """Validates a datetime column is on Sat"""
        predicate = F.dayofweek(rule.column) == 6
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_sunday(self, rule: Rule):
        """Validates a datetime column is on Sun"""
        predicate = F.dayofweek(rule.column) == 0
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_on_schedule(self, rule: Rule):
        """Validation of a datetime column between an hour interval"""
        predicate = F.hour(rule.column).between(*rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_daily(self, rule: Rule):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            day_mask = rule.value
            if not day_mask:
                day_mask = [1, 2, 3, 4, 5]

            _to_date = F.col(rule.column).cast(T.DateType())
            _split_to_table = F.table_function("split_to_table")

            date_range = dataframe.select(
                F.datediff("days", F.min(_to_date), F.max(_to_date))
            ).collect()[0][0]

            full_interval = (
                dataframe.select(
                    F.array_to_string(
                        F.array_construct(
                            *[F.min(_to_date) + i for i in range(date_range)]
                        ),
                        F.lit(" "),
                    ).alias("CUALLEE_DATE_SEQ")
                )
                .join_table_function(
                    _split_to_table(F.col("CUALLEE_DATE_SEQ"), F.lit(" ")).alias(
                        "CUALLEE_SEQ", "CUALLEE_IDX", "CUALLEE_VALUE"
                    )
                )
                .select(F.col("CUALLEE_VALUE").cast(T.DateType()).alias(rule.column))
                .filter(F.dayofweek(rule.column).isin(day_mask))
            )
            return full_interval.join(dataframe, rule.column, how="left_anti").select(
                F.when(
                    F.count(rule.column) > 0, (F.count(rule.column) * -1).cast("string")
                )
                .otherwise("True")
                .alias(key)
            )

        self.compute_instruction = ComputeInstruction(
            predicate, _execute, ComputeMethod.TRANSFORM
        )

        return self.compute_instruction

    def has_workflow(self, rule: Rule):
        """Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation"""

        predicate = None

        def _execute(dataframe: DataFrame, key: str):
            # Where [a] is source node, and [b] destination node
            edges = [F.array_construct(F.lit(a), F.lit(b)) for a, b in rule.value]
            group, event, order = rule.column
            next_event = "CUALLEE_NEXT_EVENT"
            return (
                dataframe.withColumn(
                    next_event,
                    F.lead(event).over(W.Window.partitionBy(group).orderBy(order)),
                )
                .withColumn(
                    "CUALLEE_EDGE", F.array_construct(F.col(event), F.col(next_event))
                )
                .select(
                    self._sum_predicate_to_integer(
                        reduce(
                            operator.or_,
                            [(F.col("CUALLEE_EDGE") == e) for e in edges],
                        )
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
        Tuple[Type, Type],
        Type[T.DateType],
        Type[T._NumericType],
        Type[T.TimestampType],
        Type[T.TimeType],
        Type[T.StringType],
    ],
) -> List[str]:
    """Internal method to search for column names based on data type"""
    return [
        f.name for f in dataframe.schema.fields if isinstance(f.datatype, field_type)
    ]


# def _column_set_comparison(
#     rules: Dict[str, Rule],
#     dataframe: DataFrame,
#     columns,
#     filter: Callable,
#     fn: Callable,
# ) -> Collection:
#     """Compair type of the columns passed in rules and present in dataframe."""
#     return set(
#         map(
#             str.upper,
#             cuallee_utils.get_column_set(map(columns, valfilter(filter, rules).values())),
#         )
#     ).difference(fn(dataframe))


def _compute_select_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught select method"""

    # Filter expression directed to select
    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    if not select:
        return {}

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in select.items()
            ]
        )
        .first()
        .asDict()
    )


def _compute_transform_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark transform"""

    # Filter expression directed to transform
    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())
        for k, compute_instruction in transform.items()
    }


def _get_snowflake_configurations(snowflake_env: Dict):
    return {k: os.getenv(v, None) for k, v in snowflake_env.items()}  # type: ignore


def numeric_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T._NumericType)


def string_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, (T.DateType, T.TimestampType))


def timestamp_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def validate_data_types(rules: List[Rule], dataframe: DataFrame) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""

    # COLUMNS
    # =======
    rule_match = cuallee_utils.match_columns(rules, dataframe.columns)
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


def summary(check: Check, dataframe: DataFrame) -> DataFrame:
    """Compute all rules in this check for specific data frame"""

    # Number of rows
    rows = dataframe.count()

    # Compute the expression
    select_result = _compute_select_method(compute(check._rule), dataframe)
    transform_result = _compute_transform_method(compute(check._rule), dataframe)

    unified_results = {**select_result, **transform_result}

    _calculate_violations = lambda result_column: (
        F.when(result_column == "False", F.lit(rows))
        .when(result_column == "True", F.lit(0))
        .when(result_column < 0, F.abs(result_column))
        .otherwise(F.lit(rows) - result_column.cast("long"))
    )

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "False", F.lit(0.0))
        .when(observed_column == "True", F.lit(1.0))
        .when(
            (observed_column < 0) & (F.abs(observed_column) < rows),
            1 - (F.abs(observed_column) / rows),
        )
        .when((observed_column < 0) & (F.abs(observed_column) == rows), 0.5)
        .when(
            (observed_column < 0) & (F.abs(observed_column) > rows),
            rows / F.abs(observed_column),
        )
        .otherwise(observed_column.cast(T.DoubleType()) / rows)
    )

    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    if sessions := valfilter(lambda x: isinstance(x, Session), globals()):
        snowpark = first(sessions.values())
    elif sessions := valfilter(lambda x: isinstance(x, Session), locals()):
        snowpark = first(sessions.values())
    if sessions := valfilter(lambda x: isinstance(x, SnowSession), globals()):
        snowpark = first(sessions.values())
    elif sessions := valfilter(lambda x: isinstance(x, SnowSession), locals()):
        snowpark = first(sessions.values())
    elif check.session:
        snowpark = check.session
    else:

        # Create SnowparkSession using account info
        SNOWFLAKE_ENVIRONMENT = {
            "account": "SF_ACCOUNT",
            "user": "SF_USER",
            "password": "SF_PASSWORD",
            "role": "SF_ROLE",
            "warehouse": "SF_WAREHOUSE",
            "database": "SF_DATABASE",
            "schema": "SF_SCHEMA",
        }

        if not check.config:
            check.config = _get_snowflake_configurations(SNOWFLAKE_ENVIRONMENT)

        assert set(SNOWFLAKE_ENVIRONMENT.keys()).issuperset(
            check.config.keys()
        ), "SnowFlake Environment variables not available in check configuration"

        snowpark = Session.builder.configs(check.config).create()

    computation_basis = snowpark.createDataFrame(
        [
            Row(
                index,
                rule.method,
                str(rule.column),
                str(rule.value),
                str(unified_results[hash_key]),
                rule.coverage,
            )
            for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
        ],
        schema=["id", "rule", "column", "value", "result", "pass_threshold"],
    )

    return computation_basis.select(
        F.col("id"),
        F.lit(check.date.strftime("%Y-%m-%d %H:%M:%S")).alias("timestamp"),
        F.lit(check.name).alias("check"),
        F.lit(check.level.name).alias("level"),
        F.col("column"),
        F.col("rule"),
        F.col("value"),
        F.lit(rows).alias("rows"),
        _calculate_violations(F.col("RESULT")).alias("violations"),
        _calculate_pass_rate(F.col("result")).alias("pass_rate"),
        F.col("pass_threshold").cast(T.DoubleType()).alias("pass_threshold"),
    ).withColumn(
        "status",
        _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
    )
