import enum
import hashlib
import logging
import operator
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame
from toolz import valfilter  # type: ignore

import pandas as pd  # type: ignore

from .utils import get_column_set

logger = logging.getLogger(__name__)


class CheckLevel(enum.Enum):
    WARNING = 0
    ERROR = 1


class CheckDataType(enum.Enum):
    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIMESTAMP = 4


@dataclass
class Rule:
    method: str
    column: Union[Tuple, str]
    value: Optional[Any]
    data_type: CheckDataType
    coverage: float = 1.0
    status: str = None

    def __post_init__(self):
        if (self.coverage <= 0) or (self.coverage > 1):
            raise ValueError("Coverage should be between 0 and 1")

    def __repr__(self):
        return f"Rule(method:{self.method}, column:{self.column}, value:{self.value}, data_type:{self.data_type}, coverage:{self.coverage}, status:{self.status}"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, None]
    expression: Column
    compute_method: str

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Check:
    def __init__(
        self,
        level: Union[CheckLevel, int],
        name: str,
        execution_date: datetime = datetime.today(),
    ):
        """A container of data quality rules."""
        self._rule: Dict[str, Rule] = {}
        self._compute: Dict[str, ComputeInstruction] = {}

        if isinstance(level, int):
            level = CheckLevel(level)

        self.level = level
        self.name = name
        self.date = execution_date
        self.rows = -1

    def __repr__(self):
        return f"Check(level:{self.level}, desc:{self.name}, rules:{self.sum})"

    @property
    def sum(self):
        """Collect compute, unique and union type of rules"""
        return len(self._rule.keys())

    @property
    def rules(self):
        """Returns all rules defined for check"""
        return list(self._rule.values())

    @property
    def expressions(self):
        """Returns all summary expressions for validation"""
        return list(map(lambda x: x.expression, self._compute.values()))

    @property
    def predicates(self):
        """Returns all filtering predicates for negative samples"""
        return list(
            filter(
                lambda x: x is not None,
                map(lambda x: x.predicate, self._compute.values()),
            )
        )

    def _generate_rule_key_id(
        self,
        method: str,
        column: Union[Tuple, str],
        value: Any,
        coverage: float,
    ):
        """A hash function to generate unique identifiers for rules"""
        return hashlib.blake2s(
            bytes(f"{method}{column}{value}{coverage}", "utf-8")
        ).hexdigest()

    def _generate_rule_hash(self, rule: Rule):
        """A hash funtion for unique rule identifiers"""
        return self._generate_rule_key_id(
            rule.method, rule.column, rule.value, rule.coverage
        )

    def _remove_rule_and_compute(self, key: str):
        """Remove a key from rules and compute dictionaries"""
        [
            collection.pop(key)
            for collection in [self._rule, self._compute]
            if key in collection.keys()
        ]

    def add_rule(self, method: str, *arg):
        """Add a new rule to the Check class."""
        return operator.methodcaller(method, *arg)(self)

    def delete_rule_by_key(self, keys: Union[str, List[str]]):
        """Delete rules from self._rule and self._compute dictionnary based on keys."""
        if isinstance(keys, str):
            keys = [keys]

        [self._remove_rule_and_compute(key) for key in keys]
        return self

    def delete_rule_by_attribute(
        self,
        rule_attribute: Literal["method", "column", "coverage"],
        values: Union[List[str], List[float]],
    ):
        """Delete rule based on method(s) or column name(s) or coverage value(s)."""
        if not isinstance(values, List):
            values = [values]

        _filter = lambda x: operator.attrgetter(rule_attribute)(x) in values
        [
            self._remove_rule_and_compute(key)
            for key in valfilter(_filter, self._rule).keys()
        ]
        return self

    def is_complete(self, column: str, pct: float = 1.0):
        """Validation for non-null values in column"""
        rule = Rule("is_complete", column, "N/A", CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule

        return self

    def are_complete(self, column: str, pct: float = 1.0):
        """Validation for non-null values in a group of columns"""
        if isinstance(column, List):
            column = tuple(column)
        rule = Rule("are_complete", column, "N/A", CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_unique(self, column: str, pct: float = 1.0):
        """Validation for unique values in column"""
        rule = Rule("is_unique", column, "N/A", CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def are_unique(self, column: Tuple[str], pct: float = 1.0):
        """Validation for unique values in a group of columns"""
        if isinstance(column, List):
            column = tuple(column)
        rule = Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater than value"""
        rule = Rule("is_greater_than", column, value, CheckDataType.NUMERIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater or equal than value"""
        rule = Rule(
            "is_greater_or_equal_than", column, value, CheckDataType.NUMERIC, pct
        )
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less than value"""
        rule = Rule("is_less_than", column, value, CheckDataType.NUMERIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less or equal than value"""
        rule = Rule("is_less_or_equal_than", column, value, CheckDataType.NUMERIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric column equal than value"""
        rule = Rule("is_equal_than", column, value, CheckDataType.NUMERIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_pattern(self, column: str, value: str, pct: float = 1.0):
        """Validation for string type column matching regex expression"""
        rule = Rule("has_pattern", column, value, CheckDataType.STRING, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

        # ComputeInstruction(
        #     Rule("has_pattern", column, value, CheckDataType.STRING, pct),
        #     F.sum((F.length(F.regexp_extract(column, value, 0)) > 0).cast("integer")),

    def has_min(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s minimum value"""
        rule = Rule("has_min", column, value, CheckDataType.NUMERIC)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_max(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s maximum value"""
        rule = Rule("has_max", column, value, CheckDataType.NUMERIC)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_std(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s standard deviation"""
        rule = Rule("has_std", column, value, CheckDataType.NUMERIC)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_mean(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column's average/mean"""
        rule = Rule("has_mean", column, value, CheckDataType.NUMERIC)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_between(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """Validation of a column between a range"""

        # Create tuple if user pass list
        if isinstance(value, List):
            value = tuple(value)

        rule = Rule("is_between", column, value, CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_contained_in(
        self, column: str, value: Tuple[str, int, float], pct: float = 1.0
    ):
        """Validation of column value in set of given values"""
        # Create tuple if user pass list
        if isinstance(value, List):
            value = tuple(value)

        # Check value type to later assess correct column type
        if [isinstance(v, str) for v in value]:
            check = CheckDataType.STRING
        else:
            check = CheckDataType.NUMERIC

        rule = Rule("is_contained_in", column, value, check)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def is_in(self, column: str, value: Tuple[str, int, float], pct: float = 1.0):
        """Vaidation of column value in set of given values"""
        return self.is_contained_in(column, value, pct)

    def is_on_weekday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is in a Mon-Fri time range"""
        rule = Rule("is_on_weekday", column, "Mon-Fri", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_weekday", column, "Mon-Fri", CheckDataType.DATE, pct),
        #     F.sum(F.dayofweek(f"`{column}`").between(2, 6).cast("integer")),
        # )
        return self

    def is_on_weekend(self, column: str, pct: float = 1.0):
        """Validates a datetime column is in a Sat-Sun time range"""
        rule = Rule("is_on_weekend", column, "Sat-Sun", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_weekend", column, "Sat-Sun", CheckDataType.DATE, pct),
        #     F.sum(F.dayofweek(f"`{column}`").isin([1, 7]).cast("integer")),
        # )
        return self

    def is_on_monday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Mon"""
        rule = Rule("is_on_monday", column, "Mon", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_monday", column, "Mon", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 2).cast("integer")),
        # )
        return self

    def is_on_tuesday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Tue"""
        rule = Rule("is_on_tuesday", column, "Tue", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_tuesday", column, "Tue", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 3).cast("integer")),
        # )
        return self

    def is_on_wednesday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Wed"""
        rule = Rule("is_on_wednesday", column, "Wed", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_wednesday", column, "Wed", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 3).cast("integer")),
        # )
        return self

    def is_on_thursday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Thu"""
        rule = Rule("is_on_thursday", column, "Thu", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_thursday", column, "Thu", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 4).cast("integer")),
        # )
        return self

    def is_on_friday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Fri"""
        rule = Rule("is_on_friday", column, "Fri", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_friday", column, "Fri", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 5).cast("integer")),
        # )
        return self

    def is_on_saturday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Sat"""
        rule = Rule("is_on_saturday", column, "Sat", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_saturday", column, "Sat", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 7).cast("integer")),
        # )
        return self

    def is_on_sunday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Sun"""
        rule = Rule("is_on_sunday", column, "Sun", CheckDataType.DATE, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_sunday", column, "Sun", CheckDataType.DATE, pct),
        #     F.sum((F.dayofweek(f"`{column}`") == 1).cast("integer")),
        # )
        return self

    def is_on_schedule(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """Validation of a datetime column between an hour interval"""

        # Create tuple if user pass list
        if isinstance(value, List):
            value = tuple(value)

        rule = Rule("is_on_schedule", column, value, CheckDataType.TIMESTAMP, pct)
        key = self._generate_rule_hash(rule)
        # self._compute[key] = ComputeInstruction(
        #     Rule("is_on_schedule", column, value, CheckDataType.TIMESTAMP, pct),
        #     F.sum(F.hour(column).between(*value).cast("integer")),  # type: ignore
        # )
        self._rule[key] = rule
        return self

    def has_percentile(
        self,
        column: str,
        value: float,
        percentile: float,
        precision: int = 10000,
        pct: float = 1.0,
    ):
        """Validation of a column percentile value"""
        rule = Rule(
            "has_percentile",
            column,
            (value, percentile, precision),
            CheckDataType.NUMERIC,
            pct,
        )
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_max_by(
        self, column_source: str, column_target: str, value: float, pct: float = 1.0
    ):
        """Validation of a column maximum based on other column maximum"""
        rule = Rule(
            "has_max_by",
            (column_source, column_target),
            value,
            CheckDataType.NUMERIC,
        )
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_min_by(
        self, column_source: str, column_target: str, value: float, pct: float = 1.0
    ):
        """Validation of a column minimum based on other column minimum"""
        rule = Rule(
            "has_min_by",
            (column_source, column_target),
            value,
            CheckDataType.NUMERIC,
        )
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def has_correlation(
        self, column_left: str, column_right: str, value: float, pct: float = 1.0
    ):
        """Validates the correlation between 2 columns with some tolerance"""
        rule = Rule(
            "has_correlation",
            (column_left, column_right),
            value,
            CheckDataType.NUMERIC,
        )
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        return self

    def satisfies(self, predicate: str, column: str, pct: float = 1.0):
        """Validation of a column satisfying a SQL-like predicate"""
        rule = Rule("satisfies", column, predicate, CheckDataType.AGNOSTIC, pct)
        key = self._generate_rule_hash(rule)
        self._rule[key] = rule
        # self._compute[key] = ComputeInstruction(
        #     Rule("satisfies", column, predicate, CheckDataType.AGNOSTIC, pct),
        #     F.sum(F.expr(predicate).cast("integer")),
        # )
        return self

    def has_entropy(self, column: str, value: float, tolerance: float = 0.01):
        """Validation for entropy calculation on continuous values"""
        rule = Rule("has_entropy", column, value, CheckDataType.AGNOSTIC)
        key = self._generate_rule_hash(rule)

        # def _execute(dataframe: DataFrame):
        #     return (
        #         dataframe.groupby(column)
        #         .count()
        #         .select(F.collect_list("count").alias("freq"))
        #         .select(
        #             F.col("freq"),
        #             F.aggregate("freq", F.lit(0.0), lambda a, b: a + b).alias("rows"),
        #         )
        #         .withColumn("probs", F.transform("freq", lambda x: x / F.col("rows")))
        #         .withColumn("n_labels", F.size("probs"))
        #         .withColumn("log_labels", F.log("n_labels"))
        #         .withColumn("log_prob", F.transform("probs", lambda x: F.log(x)))
        #         .withColumn(
        #             "log_classes",
        #             F.transform("probs", lambda x: F.log((x / x) * F.col("n_labels"))),
        #         )
        #         .withColumn("entropy_vals", F.arrays_zip("probs", "log_prob"))
        #         .withColumn(
        #             "product_prob",
        #             F.transform(
        #                 "entropy_vals",
        #                 lambda x: x.getItem("probs") * x.getItem("log_prob"),
        #             ),
        #         )
        #         .select(
        #             (
        #                 F.aggregate(
        #                     "product_prob", F.lit(0.0), lambda acc, x: acc + x
        #                 ).alias("p")
        #                 / F.col("log_labels")
        #                 * -1
        #             ).alias("entropy")
        #         )
        #         .select(
        #             F.expr(
        #                 f"entropy BETWEEN {value-tolerance} AND {value+tolerance}"
        #             ).alias(key)
        #         )
        #     )

        self._rule[key] = rule
        return self

    def has_weekday_continuity(self, column: str, pct: float = 1.0):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""
        rule = Rule(
            "has_weekday_continuity", column, "⊂{Mon-Fri}", CheckDataType.DATE, pct
        )
        key = self._generate_rule_hash(rule)

        # def _execute(dataframe: DataFrame):
        #     _weekdays = lambda x: x.filter(F.dayofweek(column).isin([2, 3, 4, 5, 6]))
        #     _date_only = lambda x: x.select(F.to_date(column).alias(column))
        #     full_interval = (
        #         dataframe.select(
        #             F.explode(
        #                 F.sequence(
        #                     F.min(column), F.max(column), F.expr("interval 1 day")
        #                 )
        #             ).alias(column)
        #         )
        #         .transform(_weekdays)
        #         .transform(_date_only)
        #     )
        #     return full_interval.join(
        #         dataframe.transform(_date_only), column, how="left_anti"
        #     ).select(
        #         (F.expr(f"{dataframe.count()} - count(distinct({column}))")).alias(key)
        #     )

        self._rule[key] = rule
        return self

    def validate(self, dataframe: Union[DataFrame, pd.DataFrame]):
        """Compute all rules in this check for specific data frame"""

        # Stop execution if the there is no rules in the check
        assert (
            self._rule
        ), "Check is empty. Add validations i.e. is_complete, is_unique, etc."

        rules = self._rule.values()

        # Obtain a set of columns required for rules
        # flattening str columns and tuple columns
        column_set = set(
            get_column_set(list(map(operator.attrgetter("column"), rules)))
        )

        unknown_columns = column_set.difference(set(dataframe.columns))
        assert not unknown_columns, f"Column(s): {unknown_columns} not in dataframe"

        # Check dataframe is spark dataframe
        if isinstance(dataframe, DataFrame):
            from pyspark.sql import SparkSession

            import cuallee.spark_validation as SV

            # Check SparkSession is available
            if "spark" in globals():
                # Enabler for execution in Databricks
                spark = globals()["spark"]
            else:
                spark = SparkSession.builder.getOrCreate()

            assert isinstance(
                spark, SparkSession
            ), "The function requires to pass a spark session available, or in an environment with Apache Spark"

            # Create compute dictionary
            self._compute = SV.compute(self._rule)

            # Check Spark Version
            # TODO: Requires re-implementation of imports in the the module
            # if not SV.is_observe_capable(spark.version):
            #     # NO: replace the compute_methods
            #     self._compute = {} # new with select only
            # SV._get_spark_version(self, spark)

            # Pre-Validation of data types
            assert SV.validate_data_types(
                self._rule, dataframe
            ), "Invalid data types found"

            # Compute
            return SV.summary(self, dataframe, spark)
            # _get_rule_status(self, summary)

        elif isinstance(dataframe, pd.DataFrame):
            from .pandas.pandas_validation import pd_compute_summary

            return pd_compute_summary(dataframe, self)

        #     df_observation = dataframe.observe(
        #         observation,
        #         *[
        #             compute_instruction.expression.alias(hash_key)  # type: ignore
        #             for hash_key, compute_instruction in self._compute.items()
        #         ],
        #     )
        #     rows = df_observation.count()
        #     observation_result = observation.get
        #     logger.info(observation_result)
        # else:
        #     observation_result = {}
        #     rows = dataframe.count()

        # self.rows = rows

        # unique_observe = (
        #     dataframe.select(
        #         *[
        #             compute_instrunction.expression.alias(hash_key)  # type: ignore
        #             for hash_key, compute_instrunction in self._unique.items()
        #         ]
        #     )
        #     .first()
        #     .asDict()  # type: ignore
        # )

        # union_observe = {
        #     k: operator.attrgetter(k)(compute_instruction.expression(dataframe).first())  # type: ignore
        #     for k, compute_instruction in self._union.items()
        # }

        # unified_results = {**unique_observe, **observation_result, **union_observe}

        # _calculate_pass_rate = lambda observed_column: (
        #     F.when(observed_column == "false", F.lit(0.0))
        #     .when(observed_column == "true", F.lit(1.0))
        #     .otherwise(observed_column.cast(T.DoubleType()) / self.rows)  # type: ignore
        # )
        # _evaluate_status = lambda pass_rate, pass_threshold: (
        #     F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
        # )
        # _metadata = F.create_map(
        #     *chain.from_iterable([(F.lit(k), F.lit(v)) for k, v in metadata.items()])
        # )
        # return (
        #     spark.createDataFrame(
        #         [
        #             Row(  # type: ignore
        #                 index,
        #                 compute_instruction.rule.method,
        #                 str(compute_instruction.rule.column),
        #                 str(compute_instruction.rule.value),
        #                 unified_results[hash_key],
        #                 compute_instruction.rule.coverage,
        #             )
        #             for index, (hash_key, compute_instruction) in enumerate(
        #                 unified_rules.items(), 1
        #             )
        #         ],
        #         schema="id int, rule string, column string, value string, result string, pass_threshold string",
        #     )
        #     .select(
        #         F.col("id"),
        #         F.lit(self.date.strftime("%Y-%m-%d %H:%M:%S")).alias("timestamp"),
        #         F.lit(self.name).alias("check"),
        #         F.lit(self.level.name).alias("level"),
        #         F.col("column"),
        #         F.col("rule"),
        #         F.col("value"),
        #         F.lit(rows).alias("rows"),
        #         (rows - F.col("result").cast("long")).alias("violations"),
        #         _calculate_pass_rate(F.col("result")).alias("pass_rate"),
        #         F.col("pass_threshold").cast(T.DoubleType()),
        #         F.lit(_metadata).alias("metadata"),
        #     )
        #     .withColumn(
        #         "status",
        #         _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
        #     )
        # )

    def samples(self, dataframe: DataFrame, rule_index: int = None) -> DataFrame:
        if not rule_index:
            return reduce(
                DataFrame.unionAll,
                [dataframe.filter(predicate) for predicate in self.predicates],
            ).drop_duplicates()
        elif isinstance(rule_index, int):
            return reduce(
                DataFrame.unionAll,
                [
                    dataframe.filter(predicate)
                    for index, predicate in enumerate(self.predicates, 1)
                    if rule_index == index
                ],
            )
        elif isinstance(rule_index, list):
            return reduce(
                DataFrame.unionAll,
                [
                    dataframe.filter(predicate)
                    for index, predicate in enumerate(self.predicates, 1)
                    if index in rule_index
                ],
            ).drop_duplicates()

    # def sampling(
    #     self,
    #     dataframe: DataFrame,
    #     *arg,
    #     status: str = "FAIL",
    #     method: Union[tuple[str], str] = None,
    # ) -> DataFrame:

    #     # Validate all rule

    #     # Validate DataFrame

    #     # Check dataframe is spark dataframe
    #     if isinstance(dataframe, DataFrame):
    #         from .spark.spark_validation import get_record_sample
    #         from pyspark.sql import SparkSession

    #         spark = arg[0]
    #         assert isinstance(
    #             arg[0], SparkSession
    #         ), "The function requires to pass a spark session as arg --> validate(dataframe, SparkSession)"
    #         return get_record_sample(self, dataframe, spark, status, method)
    #     else:
    #         "I cannot do anything for you! :'-("
