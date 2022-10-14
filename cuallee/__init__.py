import enum
import hashlib
import operator
import pandas as pd  # type: ignore
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union, Dict, Literal, Annotated, get_type_hints, get_origin, get_args
from toolz import valfilter  # type: ignore
from functools import wraps

from pyspark.sql import DataFrame, Column

from cuallee.helper_functions import _delete_dict_entry


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
class ValueRange:
    min: float
    max: float
    default_value: float = 1.0


def check_annotations(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        """ Perform runtime annotation checking for ValueRange type"""
        # Get type hints from function
        type_hints = get_type_hints(func, include_extras=True)
        for param, hint in type_hints.items():
            # Only process annotated types
            if get_origin(hint) is Annotated:
                # Get base type and additional arguments
                hint_type, hint_args = get_args(hint)
                # Process ValueRange arg only
                if isinstance(hint_args, ValueRange):
                    # If no pct passed, used default pct value
                    if kwargs.get(param) == None:
                        actual_value = hint_args.default_value 
                    else:
                        actual_value = kwargs.get(param)
                    if not (hint_args.min < actual_value <= hint_args.max):
                        raise ValueError(f'Pct value must be in range ]{hint_args.min}, {hint_args.max}]')
        # execute function once all checks passed
        return func(*args, **kwargs)

    return wrapped


@dataclass
class Rule:
    method: str
    column: Union[Tuple, str]
    value: Optional[Any]
    data_type: CheckDataType
    coverage: Annotated[float, ValueRange(0.0, 1.0)]
    status: str = None

    def __repr__(self):
        return f"Rule(method:{self.method}, column:{self.column}, value:{self.value}, data_type:{self.data_type}, coverage:{self.coverage}, status:{self.status}"


@dataclass
class ComputeInstruction:
    predicate: Column
    expression: Column
    compute_method: str


class Check:
    def __init__(
        self, level: CheckLevel, name: str, execution_date: datetime = datetime.today()
    ):
        self._rule: Dict[str, Rule] = {}
        self._compute: Dict[str, ComputeInstruction] = {}
        self._validation: str = ""
        self.level = level
        self.name = name
        self.date = execution_date
        self.rows = -1

    def __repr__(self):
        return f"Check(level:{self.level}, desc:{self.name}, rules:{len(self._rule)})"

    def _add_rule(self, method: str, *arg):
        """Add a new rule to the Check class."""
        f = operator.methodcaller(method, *arg)
        return f(self)

    def _delete_rule_by_key(self, keys: List[str]):
        """Delete rules from self._rule and self._compute dictionnary based on keys."""
        if isinstance(keys, str):
            keys = [keys]
        _delete_dict_entry(keys, self._rule, self._compute)
        return self

    def _delete_rule_by(self, attr: Literal['method', 'column', 'coverage'], values: Union[List[str], List[float]]):
        """Delete rule based on method(s) or column name(s) or coverage value(s)."""
        if not isinstance(values, List):
            values = [values]

            if attr == 'method':
                _filter = lambda x: x.method in values
            elif attr == 'column':
                _filter = lambda x: x.column in values
            else:
                _filter = lambda x: x.coverage in values

            _delete_dict_entry(
                valfilter(_filter, self._rule).keys(), self._rule, self._compute
            )
        return self

    def _delete_individual_rule(
        self,
        method: str,
        column: Union[Tuple, str],
        value: Optional[Any],
        coverage: float = 1.0,
    ):
        _method = lambda x: x.method == method
        _column = lambda x: x.column == column
        _value = lambda x: x.value == value
        _coverage = lambda x: x.coverage == coverage 
        _filter = _method &_column & _value & _coverage
        _delete_dict_entry(valfilter(_filter, self._rule).keys(), self._rule, self._compute)
        return self

    def _modify_rule_by_key(self, key):
        pass

    def _generate_rule_key_id(
        self,
        method: str,
        column: Union[Tuple, str],
        value: Any,
        coverage: float,
    ):
        return hashlib.blake2s(
            bytes(f"{method}{column}{value}{coverage}", "utf-8")
        ).hexdigest()

    @staticmethod
    def _compute_columns(columns: Union[str, List[str]]) -> List[str]:
        """Confirm that all compute columns exists in dataframe"""

        def _normalize_columns(col: Union[str, List[str]], agg: List[str]) -> List[str]:
            """Recursive consilidation of compute columns"""
            if isinstance(col, str):
                agg.append(col)
            else:
                [_normalize_columns(inner_col, agg) for inner_col in col]
            return agg

        return _normalize_columns(columns, [])

    @check_annotations
    def is_complete(self, column: str, /,pct: Annotated[float, ValueRange(0.0, 1.0)] = 1.0):
        """Validation for non-null values in column"""
        key = self._generate_rule_key_id("is_complete", column, "N/A", pct)
        self._rule[key] = Rule(
            "is_complete", column, "N/A", CheckDataType.AGNOSTIC, pct
        )
        return self

    def are_complete(self, column: str, pct: float = 1.0):
        """Validation for non-null values in a group of columns"""
        # if isinstance(column, List):
        #    column = tuple(column)
        key = self._generate_rule_key_id("are_complete", column, "N/A", pct)
        self._rule[key] = Rule(
            "are_complete", column, "N/A", CheckDataType.AGNOSTIC, pct
        )
        return self

    def is_unique(self, column: str, pct: float = 1.0):
        """Validation for unique values in column"""
        key = self._generate_rule_key_id("is_unique", column, "N/A", pct)
        self._rule[key] = Rule("is_unique", column, "N/A", CheckDataType.AGNOSTIC, pct)
        return self

    def are_unique(self, column: Tuple[str], pct: float = 1.0):
        """Validation for unique values in a group of columns"""
        if isinstance(column, List):
            column = tuple(column)
        key = self._generate_rule_key_id("are_unique", column, "N/A", pct)
        self._rule[key] = Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct)
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater than value"""
        key = self._generate_rule_key_id("is_greater_than", column, value, pct)
        self._rule[key] = Rule(
            "is_greater_than", column, value, CheckDataType.NUMERIC, pct
        )
        return self

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater or equal than value"""
        key = self._generate_rule_key_id("is_greater_or_equal_than", column, value, pct)
        self._rule[key] = Rule(
            "is_greater_or_equal_than", column, value, CheckDataType.NUMERIC, pct
        )
        return self

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less than value"""
        key = self._generate_rule_key_id("is_less_than", column, value, pct)
        self._rule[key] = Rule(
            "is_less_than", column, value, CheckDataType.NUMERIC, pct
        )
        return self

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less or equal than value"""
        key = self._generate_rule_key_id("is_less_or_equal_than", column, value, pct)
        self._rule[key] = Rule(
            "is_less_or_equal_than", column, value, CheckDataType.NUMERIC, pct
        )
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric column equal than value"""
        key = self._generate_rule_key_id("is_equal", column, value, pct)
        self._rule[key] = Rule("is_equal", column, value, CheckDataType.NUMERIC, pct)
        return self

    def matches_regex(self, column: str, value: str, pct: float = 1.0):
        """Validation for string type column matching regex expression"""
        key = self._generate_rule_key_id("matches_regex", column, value, pct)
        self._rule[key] = Rule(
            "matches_regex", column, value, CheckDataType.STRING, pct
        )
        return self

    def has_min(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s minimum value"""
        key = self._generate_rule_key_id("has_min", column, value, pct)
        self._rule[key] = Rule("has_min", column, value, CheckDataType.NUMERIC)
        return self

    def has_max(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s maximum value"""
        key = self._generate_rule_key_id("has_max", column, value, pct)
        self._rule[key] = Rule("has_max", column, value, CheckDataType.NUMERIC)
        return self

    def has_std(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column’s standard deviation"""
        key = self._generate_rule_key_id("has_std", column, value, pct)
        self._rule[key] = Rule("has_std", column, value, CheckDataType.NUMERIC)
        return self

    def has_mean(self, column: str, value: float, pct: float = 1.0):
        """Validation of a column's average/mean"""
        key = self._generate_rule_key_id("has_mean", column, value, pct)
        self._rule[key] = Rule("has_mean", column, value, CheckDataType.NUMERIC)
        return self

    def is_between(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """Validation of a column between a range"""

        # Create tuple if user pass list
        if isinstance(value, List):
            value = tuple(value)

        key = self._generate_rule_key_id("is_between", column, value, pct)
        self._rule[key] = Rule("is_between", column, value, CheckDataType.AGNOSTIC, pct)
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

        key = self._generate_rule_key_id("is_contained_in", column, value, pct)
        self._rule[key] = Rule("is_contained_in", column, value, check)
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
        key = self._generate_rule_key_id(
            "has_percentile", column, (value, percentile, precision), pct
        )
        self._rule[key] = Rule(
            "has_percentile",
            column,
            (value, percentile, precision),
            CheckDataType.NUMERIC,
            pct,
        )
        return self

    def has_max_by(
        self, column_source: str, column_target: str, value: float, pct: float = 1.0
    ):
        """Validation of a column maximum based on other column maximum"""
        key = self._generate_rule_key_id(
            "has_max_by", (column_source, column_target), value, pct
        )
        self._rule[key] = Rule(
            "has_max_by",
            (column_source, column_target),
            value,
            CheckDataType.NUMERIC,
        )
        return self

    def has_min_by(
        self, column_source: str, column_target: str, value: float, pct: float = 1.0
    ):
        """Validation of a column minimum based on other column minimum"""
        key = self._generate_rule_key_id(
            "has_min_by", (column_source, column_target), value, pct
        )
        self._rule[key] = Rule(
            "has_min_by",
            (column_source, column_target),
            value,
            CheckDataType.NUMERIC,
        )
        return self

    def has_correlation(
        self, column_left: str, column_right: str, value: float, pct: float = 1.0
    ):
        """Validates the correlation between 2 columns with some tolerance"""

        key = self._generate_rule_key_id(
            "has_correlation", (column_left, column_right), value, pct
        )
        self._rule[key] = Rule(
            "has_correlation",
            (column_left, column_right),
            value,
            CheckDataType.NUMERIC,
        )
        return self

    def satisfies(self, predicate: str, pct: float = 1.0):
        """Validation of a column satisfying a SQL-like predicate"""
        key = self._generate_rule_key_id("satisfies", "N/A", predicate, pct)
        self._rule[key] = Rule("satisfies", "N/A", predicate, CheckDataType.AGNOSTIC)
        return self

    def validate(self, dataframe: DataFrame, *arg) -> DataFrame:
        """Compute all rules in this check for specific data frame"""

        # Check the dictionnary is not empty
        assert (
            self._rule
        ), "Check is empty. Add validations i.e. is_complete, is_unique, etc."

        rule_expressions = self._rule.values()

        # Pre-validate column names
        column_set = set(
            Check._compute_columns(
                list(map(operator.attrgetter("column"), rule_expressions))
            )
        )
        unknown_columns = column_set.difference(set(dataframe.columns))
        assert not unknown_columns, f"Column(s): {unknown_columns} not in dataframe"

        # Check dataframe is spark dataframe
        if isinstance(dataframe, DataFrame):
            from .spark.spark_validation import (
                compute_summary,
                _get_rule_status,
                _get_spark_version,
                _get_compute_dict,
                _validate_dataTypes,
            )
            from pyspark.sql import SparkSession

            # Check SparkSession is available
            spark = arg[0]
            assert isinstance(
                arg[0], SparkSession
            ), "The function requires to pass a spark session as arg --> validate(dataframe, SparkSession)"

            # Create compute dictionary
            _get_compute_dict(self)

            # Check Spark Version
            _get_spark_version(self, spark)

            # Pre-Validation of data types
            _validate_dataTypes(self, dataframe)

            # Compute
            summary = compute_summary(self, dataframe, spark)
            _get_rule_status(self, summary)
            return summary
        elif isinstance(dataframe, pd.DataFrame):
            from .pandas.pandas_validation import pd_compute_summary

            return pd_compute_summary(dataframe, self)

    def sampling(
        self,
        dataframe: DataFrame,
        *arg,
        status: str = "FAIL",
        method: Union[tuple[str], str] = None,
    ) -> DataFrame:

        # Validate all rule

        # Validate DataFrame

        # Check dataframe is spark dataframe
        if isinstance(dataframe, DataFrame):
            from .spark.spark_validation import get_record_sample
            from pyspark.sql import SparkSession

            spark = arg[0]
            assert isinstance(
                arg[0], SparkSession
            ), "The function requires to pass a spark session as arg --> validate(dataframe, SparkSession)"
            return get_record_sample(self, dataframe, spark, status, method)
        else:
            "I cannot do anything for you! :-("
