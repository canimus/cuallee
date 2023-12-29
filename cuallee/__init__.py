import enum
import hashlib
import importlib
import logging
import operator
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import ModuleType
from typing import Any, Dict, List, Literal, Optional, Protocol, Tuple, Union
from numbers import Number
from .iso.checks import ISO

from colorama import Fore, Style  # type: ignore
from toolz import valfilter  # type: ignore
import numpy as np

logger = logging.getLogger("cuallee")

# Verify Libraries Available
# ==========================
try:
    from pandas import DataFrame as pandas_dataframe  # type: ignore

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " Pandas")
except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " Pandas")

try:
    from polars.dataframe.frame import DataFrame as polars_dataframe  # type: ignore

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " Polars")
except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " Polars")

try:
    from pyspark.sql import DataFrame as pyspark_dataframe

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " PySpark")

except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " PySpark")

try:
    from snowflake.snowpark import DataFrame as snowpark_dataframe  # type: ignore

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " Snowpark")
except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " Snowpark")

try:
    from duckdb import DuckDBPyConnection as duckdb_dataframe  # type: ignore

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " DuckDB")
except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " DuckDB")

try:
    from google.cloud import bigquery

    logger.debug(Fore.GREEN + "[OK]" + Fore.WHITE + " BigQuery")
except:
    logger.debug(Fore.RED + "[KO]" + Fore.WHITE + " BigQuery")


logger.debug(Style.RESET_ALL)

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
    DUO = 5


class CheckStatus(enum.Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    NO_RUN = "NO_RUN"


@dataclass
class Rule:
    method: str
    column: Union[str, List[str], Tuple[str, str]]
    value: Optional[Any]
    data_type: CheckDataType
    coverage: float = 1.0
    options: Union[List[Tuple], None] = None
    status: Union[str, None] = None

    @property
    def settings(self) -> dict:
        return dict(self.options)

    @property
    def key(self):
        return (
            hashlib.blake2s(
                bytes(
                    f"{self.method}{self.column}{self.value}{self.options}{self.coverage}",
                    "utf-8",
                )
            )
            .hexdigest()
            .upper()
        )

    def __post_init__(self):
        if (self.coverage <= 0) or (self.coverage > 1):
            raise ValueError("Coverage should be between 0 and 1")

        if isinstance(self.column, List):
            self.column = tuple(self.column)

        if isinstance(self.value, List):
            self.value = tuple(self.value)

        if isinstance(self.value, Tuple) & (self.data_type == CheckDataType.AGNOSTIC):
            # All values can only be of one data type in a rule
            if len(Counter(map(type, self.value)).keys()) > 1:
                raise ValueError("Data types in rule values are inconsistent")

    def __repr__(self):
        return f"Rule(method:{self.method}, column:{self.column}, value:{self.value}, data_type:{self.data_type}, coverage:{self.coverage}, status:{self.status}"

    def __rshift__(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        rule_dict[self.key] = self
        return rule_dict


class ComputeEngine(Protocol):
    def compute(self, rules: Dict[str, Rule]) -> bool:
        """Returns compute instructions for each rule"""

    def validate_data_types(self, rules: List[Rule], dataframe: Any) -> bool:
        """Validates that all data types from checks match the dataframe with data"""

    def summary(self, check: Any, dataframe: Any) -> Any:
        """Computes all predicates and expressions for check summary"""

        
class Check:
    def __init__(
        self,
        level: Union[CheckLevel, int],
        name: str,
        *,
        execution_date: datetime = datetime.today(),
        table_name: str = None,
    ):
        """A container of data quality rules."""
        self._rule: Dict[str, Rule] = {}
        # TODO: Should be a compute engine protocol
        self.compute_engine: ModuleType

        if isinstance(level, int):
            # When the user is lazy and wants to do WARN=0, or ERR=1
            level = CheckLevel(level)

        self.level = level
        self.name = name
        self.date = execution_date
        self.rows = -1
        self.config: Dict[str, str] = {}
        self.table_name = table_name
        self.iso = ISO(self)

    def __repr__(self):
        standard = f"Check(level:{self.level}, desc:{self.name}, rules:{self.sum})"
        if self.table_name:
            standard += f" / table:{self.table_name}"
        return standard

    @property
    def sum(self):
        """Collect compute, unique and union type of rules"""
        return len(self._rule.keys())

    @property
    def rules(self):
        """Returns all rules defined for check"""
        return list(self._rule.values())

    @property
    def keys(self):
        """Returns blake2s unique identifiers of rules"""
        return list(self._rule.keys())

    @property
    def empty(self):
        """True when no rules are added in the check"""
        return len(self.rules) == 0

    def _remove_rule_generic(self, key: str):
        """Remove a key from rules and compute dictionaries"""
        if key in self._rule:
            self._rule.pop(key)

    def add_rule(self, method: str, *arg):
        """Add a new rule to the Check class."""
        return operator.methodcaller(method, *arg)(self)

    def delete_rule_by_key(self, keys: Union[str, List[str]]):
        """Delete rules from self._rule and self._compute dictionnary based on keys."""
        if isinstance(keys, str):
            keys = [keys]

        [self._remove_rule_generic(key) for key in keys]
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
            self._remove_rule_generic(key)
            for key in valfilter(_filter, self._rule).keys()
        ]
        return self

    def adjust_rule_coverage(self, rule_index: int, rule_coverage: float):
        """Targeted for adjusting the predicate/rows ratio or making rules less strict"""
        target_rule = self.rules[rule_index]
        old_key = target_rule.key
        target_rule = self._rule.pop(old_key)
        target_rule.coverage = rule_coverage
        target_rule >> self._rule
        return self

    def is_complete(self, column: str, pct: float = 1.0):
        """Validation for non-null values in column"""
        Rule("is_complete", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def are_complete(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """Validation for non-null values in a group of columns"""
        Rule("are_complete", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_unique(self, column: str, pct: float = 1.0):
        """Validation for unique values in column"""
        Rule("is_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_primary_key(self, column: str, pct: float = 1.0):
        """Validation for unique values in column"""
        Rule("is_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def are_unique(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """Validation for unique values in a group of columns"""
        Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_composite_key(
        self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0
    ):
        """Validation for unique values in a group of columns"""
        Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater than value"""
        Rule("is_greater_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def is_positive(self, column: str, pct: float = 1.0):
        """Validation for numeric greater than zero"""
        return self.is_greater_than(column, 0, pct)

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric greater or equal than value"""
        (
            Rule("is_greater_or_equal_than", column, value, CheckDataType.NUMERIC, pct)
            >> self._rule
        )
        return self

    def is_in_millions(self, column: str, pct: float = 1.0):
        """Validates that a column has values greater than 1M"""
        return self.is_greater_or_equal_than(column, 1e6, pct)

    def is_in_billions(self, column: str, pct: float = 1.0):
        """Validates that a column has values greater than 1B"""
        return self.is_greater_or_equal_than(column, 1e9, pct)

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less than value"""
        Rule("is_less_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def is_negative(self, column: str, pct: float = 1.0):
        """Validation for numeric less than zero"""
        return self.is_less_than(column, 0, pct)

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric less or equal than value"""
        (
            Rule("is_less_or_equal_than", column, value, CheckDataType.NUMERIC, pct)
            >> self._rule
        )
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        """Validation for numeric column equal than value"""
        Rule("is_equal_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def has_pattern(self, column: str, value: str, pct: float = 1.0):
        """Validation for string type column matching regex expression"""
        Rule("has_pattern", column, value, CheckDataType.STRING, pct) >> self._rule
        return self

    def is_legit(self, column: str, pct: float = 1.0):
        """Validation for string type having none space chars. Useful for CSV reading"""
        Rule("has_pattern", column, "^\S+$", CheckDataType.STRING, pct) >> self._rule
        return self

    def has_min(self, column: str, value: float):
        """Validation of a column’s minimum value"""
        Rule("has_min", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_max(self, column: str, value: float):
        """Validation of a column’s maximum value"""
        Rule("has_max", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_std(self, column: str, value: float):
        """Validation of a column’s standard deviation"""
        Rule("has_std", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_mean(self, column: str, value: float):
        """Validation of a column's average/mean"""
        Rule("has_mean", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_sum(self, column: str, value: float):
        """Validation of a sum of all values of a column"""
        Rule("has_sum", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def is_between(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """Validation of a column between a range"""
        Rule("is_between", column, value, CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_contained_in(
        self,
        column: str,
        value: Union[List, Tuple],
        pct: float = 1.0,
    ):
        """Validation of column value in set of given values"""

        (
            Rule("is_contained_in", column, value, CheckDataType.AGNOSTIC, pct)
            >> self._rule
        )

        return self

    def is_in(self, column: str, value: Tuple[str, int, float], pct: float = 1.0):
        """Vaidation of column value in set of given values"""
        return self.is_contained_in(column, value, pct)

    def is_t_minus_n(self, column: str, value: int, pct: float = 1.0):
        """Validate that date is yesterday"""
        yesterday = datetime.utcnow() - timedelta(days=value)
        return self.is_in(column, tuple([yesterday.strftime("%Y-%m-%d")]), pct)

    def is_t_minus_1(self, column: str, pct: float = 1.0):
        """Validate that date is yesterday"""
        return self.is_t_minus_n(column, 1, pct)

    def is_t_minus_2(self, column: str, pct: float = 1.0):
        """Validate that date is 2 days ago"""
        return self.is_t_minus_n(column, 2, pct)

    def is_t_minus_3(self, column: str, pct: float = 1.0):
        """Validate that date is 3 days ago"""
        return self.is_t_minus_n(column, 3, pct)

    def is_yesterday(self, column: str, pct: float = 1.0):
        """Validate that date is yesterday"""
        return self.is_t_minus_1(column, pct)

    def is_today(self, column: str, pct: float = 1.0):
        """Validate that date is today"""
        return self.is_t_minus_n(column, 0, pct)

    def has_percentile(
        self, column: str, value: float, percentile: float, precision: int = 10000
    ):
        """Validation of a column percentile value"""
        (
            Rule(
                "has_percentile",
                column,
                value,
                CheckDataType.NUMERIC,
                options=[
                    tuple(["percentile", percentile]),
                    tuple(["precision", precision]),
                ],
            )
            >> self._rule
        )
        return self

    def is_inside_interquartile_range(
        self, column: str, value: List[float] = [0.25, 0.75], pct: float = 1.0
    ):
        """Validates a number resides inside the Q3 - Q1 range of values"""
        (
            Rule(
                "is_inside_interquartile_range",
                column,
                value,
                CheckDataType.NUMERIC,
                pct,
            )
            >> self._rule
        )
        return self

    def has_max_by(
        self, column_source: str, column_target: str, value: Union[float, str]
    ):
        """Validation of a column value based on another column maximum"""
        (
            Rule(
                "has_max_by",
                [column_source, column_target],
                value,
                CheckDataType.DUO,
            )
            >> self._rule
        )
        return self

    def has_min_by(
        self, column_source: str, column_target: str, value: Union[float, str]
    ):
        """Validation of a column value based on another column minimum"""
        (
            Rule(
                "has_min_by",
                [column_source, column_target],
                value,
                CheckDataType.DUO,
            )
            >> self._rule
        )
        return self

    def has_correlation(self, column_left: str, column_right: str, value: float):
        """Validates the correlation between 2 columns with some tolerance"""
        (
            Rule(
                "has_correlation",
                [column_left, column_right],
                value,
                CheckDataType.NUMERIC,
            )
            >> self._rule
        )
        return self

    def satisfies(self, column: str, predicate: str, pct: float = 1.0):
        """Validation of a column satisfying a SQL-like predicate"""
        Rule("satisfies", column, predicate, CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def has_entropy(self, column: str, value: float, tolerance: float = 0.01):
        """Validation for entropy calculation on continuous values"""
        (
            Rule(
                "has_entropy",
                column,
                value,
                CheckDataType.AGNOSTIC,
                options=[tuple(["tolerance", tolerance])],
            )
            >> self._rule
        )
        return self

    def is_on_weekday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is in a Mon-Fri time range"""
        Rule("is_on_weekday", column, "Mon-Fri", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_weekend(self, column: str, pct: float = 1.0):
        """Validates a datetime column is in a Sat-Sun time range"""
        Rule("is_on_weekend", column, "Sat-Sun", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_monday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Mon"""
        Rule("is_on_monday", column, "Mon", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_tuesday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Tue"""
        Rule("is_on_tuesday", column, "Tue", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_wednesday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Wed"""
        Rule("is_on_wednesday", column, "Wed", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_thursday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Thu"""
        Rule("is_on_thursday", column, "Thu", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_friday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Fri"""
        Rule("is_on_friday", column, "Fri", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_saturday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Sat"""
        Rule("is_on_saturday", column, "Sat", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_sunday(self, column: str, pct: float = 1.0):
        """Validates a datetime column is on Sun"""
        Rule("is_on_sunday", column, "Sun", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_schedule(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """Validation of a datetime column between an hour interval"""
        (
            Rule("is_on_schedule", column, value, CheckDataType.TIMESTAMP, pct)
            >> self._rule
        )
        return self

    def is_daily(
        self, column: str, value: Union[None, List[int]] = None, pct: float = 1.0
    ):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""
        (Rule("is_daily", column, value, CheckDataType.DATE, pct) >> self._rule)
        return self

    def has_workflow(
        self,
        column_group: str,
        column_event: str,
        column_order: str,
        edges: List[Tuple[str]],
        pct: float = 1.0,
    ):
        """Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation"""
        (
            Rule(
                "has_workflow",
                [column_group, column_event, column_order],
                edges,
                CheckDataType.AGNOSTIC,
                pct,
            )
            >> self._rule
        )
        return self

    def validate(self, dataframe: Any):
        """Compute all rules in this check for specific data frame"""

        # Stop execution if the there is no rules in the check
        assert not self.empty, "Check is empty. Try adding some rules?"

        # When dataframe is PySpark DataFrame API
        if "pyspark_dataframe" in globals() and isinstance(
            dataframe, pyspark_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.pyspark_validation")

        # When dataframe is Pandas DataFrame API
        elif "pandas_dataframe" in globals() and isinstance(
            dataframe, pandas_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.pandas_validation")

        # When dataframe is Snowpark DataFrame API
        elif "snowpark_dataframe" in globals() and isinstance(
            dataframe, snowpark_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.snowpark_validation")

        elif "duckdb_dataframe" in globals() and isinstance(
            dataframe, duckdb_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.duckdb_validation")

        # TODO: BigQuery source (pandas DataFrame/ json / file / uri)
        elif "bigquery" in globals() and isinstance(dataframe, bigquery.table.Table):
            self.compute_engine = importlib.import_module("cuallee.bigquery_validation")

        elif "polars_dataframe" in globals() and isinstance(
            dataframe, polars_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.polars_validation")

        assert self.compute_engine.validate_data_types(
            self.rules, dataframe
        ), "Invalid data types between rules and dataframe"
        return self.compute_engine.summary(self, dataframe)

    

class Control():
    @staticmethod
    def completeness(dataframe):
        """Control of null values on data frames"""
        check = Check(CheckLevel.WARNING, "Completeness")
        [check.is_complete(c) for c in dataframe.columns];
        return check.validate(dataframe)