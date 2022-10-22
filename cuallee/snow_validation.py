import enum
import operator
import snowflake.snowpark.functions as F  # type: ignore
import snowflake.snowpark.types as T  # type: ignore

from typing import Union, Dict, Collection, Type, Callable, Optional, Any, Tuple
from dataclasses import dataclass
from snowflake.snowpark import DataFrame, Column, Session, Row  # type: ignore
from toolz import valfilter  # type: ignore
from functools import reduce

from cuallee import Check, Rule, CheckDataType
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
        self.compute_instruction = ComputeInstruction

    def __repr__(self):
        return self.compute_instruction

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    def _single_value_rule(
        self,
        column: Union[Tuple, str],
        value: Optional[Any],
        operator: Callable,
    ):
        return operator(F.col(column), value)

    def _stats_fn_rule(
        self,
        column: Union[Tuple, str],
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

    def is_between(self, rule: Rule):
        """Validation of a column between a range"""
        predicate = F.col(rule.column).between(*rule.value)
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_contained_in(self, rule: Rule):  # TODO: Type error
        """Validation of column value in set of given values"""
        predicate = F.col(rule.column).isin(list(rule.value))
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate.cast(T.LongType())),
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
                rule.value[1],
                rule.value[2],  
            ).eqNullSafe(rule.value[0]),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    # TODO: min_by
    # def has_min_by(self, rule: Rule):  # To Do with Predicate
    #    """Validation of a column minimum based on other column minimum"""
    #    predicate = F.min_by(rule.column[1], rule.column[0]) == rule.value
    #    self.compute_instruction = ComputeInstruction(
    #        predicate,
    #        F.min_by(rule.column[1], rule.column[0]) == rule.value,
    #        "observe",
    #    )
    #    return self.compute_instruction

    # TODO: max_by
    # def has_max_by(self, rule: Rule):  # To Do with Predicate
    #    """Validation of a column maximum based on other column maximum"""
    #    predicate = None  # TODO: Does this function has a predicate?
    #    self.compute_instruction = ComputeInstruction(
    #        predicate,
    #        F.max_by(rule.column[1], rule.column[0]) == rule.value,
    #        "observe",
    #    )
    #    return self.compute_instruction

    def has_correlation(self, rule: Rule):
        """Validates the correlation between 2 columns with some tolerance"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.corr(
                F.col(rule.column[0]).cast(T.DoubleType()),
                F.col(rule.column[1]).cast(T.DoubleType()),
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

    # def has_entropy(self, rule: Rule):
    #     """Validation for entropy calculation on continuous values"""
    #     predicate = None

    #     def _execute(dataframe: DataFrame, key: str):
    #         return (
    #             dataframe.groupby(rule.column)
    #             .count()
    #             .select(F.collect_list("count").alias("freq"))
    #             .select(
    #                 F.col("freq"),
    #                 F.aggregate("freq", F.lit(0.0), lambda a, b: a + b).alias("rows"),
    #             )
    #             .withColumn("probs", F.transform("freq", lambda x: x / F.col("rows")))
    #             .withColumn("n_labels", F.size("probs"))
    #             .withColumn("log_labels", F.log("n_labels"))
    #             .withColumn("log_prob", F.transform("probs", lambda x: F.log(x)))
    #             .withColumn(
    #                 "log_classes",
    #                 F.transform("probs", lambda x: F.log((x / x) * F.col("n_labels"))),
    #             )
    #             .withColumn("entropy_vals", F.arrays_zip("probs", "log_prob"))
    #             .withColumn(
    #                 "product_prob",
    #                 F.transform(
    #                     "entropy_vals",
    #                     lambda x: x.getItem("probs") * x.getItem("log_prob"),
    #                 ),
    #             )
    #             .select(
    #                 (
    #                     F.aggregate(
    #                         "product_prob", F.lit(0.0), lambda acc, x: acc + x
    #                     ).alias("p")
    #                     / F.col("log_labels")
    #                     * -1
    #                 ).alias("entropy")
    #             )
    #             .select(
    #                 F.expr(
    #                     f"entropy BETWEEN {rule.value[0]-rule.value[1]} AND {rule.value[0]+rule.value[1]}"
    #                 ).alias(key)
    #             )
    #         )

    #     self.compute_instruction = ComputeInstruction(
    #         predicate,
    #         _execute,
    #         "transform",
    #     )
    #     return self.compute_instruction

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
            _weekdays = lambda x: x.filter(
                F.dayofweek(rule.column).isin([1, 2, 3, 4, 5])  # type: ignore
            )
            _date_only = lambda x: x.select(F.to_date(rule.column).alias(rule.column))  # type: ignore
            # full_interval = (
            #     dataframe.select(
            #         F.explode(
            #             F.sequence(
            #                 F.min(F.col(f"`{rule.column}`")),  # type: ignore
            #                 F.max(F.col(f"`{rule.column}`")),  # type: ignore
            #                 F.expr("interval 1 day"),
            #             )
            #         ).alias(
            #             f"{rule.column}"
            #         )  # type: ignore
            #     )
            #     .transform(_weekdays)
            #     .transform(_date_only)
            # )
            # return full_interval.join(  # type: ignore
            #     dataframe.transform(_date_only), rule.column, how="left_anti"  # type: ignore
            # ).select(
            #     (F.expr(f"{dataframe.count()} - count(distinct({rule.column}))")).alias(
            #         key
            #     )
            # )

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
) -> Collection:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.datatype, field_type)]
    )


def _column_set_comparison(
    rules: Dict[str, Rule],
    dataframe: DataFrame,
    columns,
    filter: Callable,
    fn: Callable,
) -> Collection:
    """Compair type of the columns passed in rules and present in dataframe."""
    return set(
        map(
            str.upper,
            cuallee_utils.get_column_set(map(columns, valfilter(filter, rules).values())),  # type: ignore
        )
    ).difference(fn(dataframe))


def _compute_select_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught select method"""

    # Filter expression directed to select
    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key.upper())
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
    _transform = lambda x: x.compute_method == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())  # type: ignore
        for k, compute_instruction in transform.items()
    }


def numeric_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T._NumericType)


def string_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, (T.DateType, T.TimestampType))


def timestamp_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def validate_data_types(rules: Dict[str, Rule], dataframe: DataFrame) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""
    _col = operator.attrgetter("column")
    _numeric = lambda x: x.data_type.name == CheckDataType.NUMERIC.name
    _date = lambda x: x.data_type.name == CheckDataType.DATE.name
    _timestamp = lambda x: x.data_type.name == CheckDataType.TIMESTAMP.name
    _string = lambda x: x.data_type.name == CheckDataType.STRING.name
    # Numeric Validation
    non_numeric = _column_set_comparison(
        rules, dataframe, _col, _numeric, numeric_fields
    )
    assert len(non_numeric) == 0, f"Column(s): {non_numeric} are not numeric"
    # String Validation
    non_string = _column_set_comparison(rules, dataframe, _col, _string, string_fields)
    assert len(non_string) == 0, f"Column(s): {non_string} are not strings"
    # Date Validation
    non_date = _column_set_comparison(rules, dataframe, _col, _date, date_fields)
    assert len(non_date) == 0, f"Column(s): {non_date} are not dates"
    # Timestamp validation
    non_timestamp = _column_set_comparison(
        rules, dataframe, _col, _timestamp, timestamp_fields
    )
    assert len(non_timestamp) == 0, f"Column(s): {non_timestamp} are not timestamps"
    return True


def summary(check: Check, dataframe: DataFrame) -> DataFrame:
    """Compute all rules in this check for specific data frame"""

    # Number of rows
    rows = dataframe.count()

    # Compute the expression
    select_result = _compute_select_method(
        check._compute, dataframe
    )  # TODO: Check with Herminio to remove the Compute Instruction in the __init__.py
    transform_result = _compute_transform_method(check._compute, dataframe)

    unified_results = {**select_result, **transform_result}

    _calculate_violations = lambda result_column: (
        F.when(result_column == "False", F.lit(rows))
        .when(result_column == "True", F.lit(0))
        .otherwise(F.lit(rows) - result_column.cast("long"))
    )

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "False", F.lit(0.0))
        .when(observed_column == "True", F.lit(1.0))
        .otherwise(observed_column.cast(T.DoubleType()) / rows)  # type: ignore
    )

    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    # Create SnowparkSession
    snowpark = Session.builder.configs(check.config).create()

    computation_basis = snowpark.createDataFrame(
        [
            Row(
                index,
                rule.method,
                str(rule.column),
                str(rule.value),
                str(unified_results[hash_key.upper()]),
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
