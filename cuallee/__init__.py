import enum
import hashlib
import importlib
import logging
import operator
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, Dict, List, Literal, Optional, Protocol, Tuple, Union, Callable
from toolz import compose, valfilter  # type: ignore
from toolz.curried import map as map_curried

logger = logging.getLogger("cuallee")
__version__ = "0.12.3"
# Verify Libraries Available
# ==========================
try:
    from pandas import DataFrame as pandas_dataframe  # type: ignore
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: Pandas")

try:
    from polars.dataframe.frame import DataFrame as polars_dataframe  # type: ignore
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: Polars")

try:
    from pyspark.sql import DataFrame as pyspark_dataframe
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: PySpark")

try:
    from pyspark.sql.connect.dataframe import DataFrame as pyspark_connect_dataframe
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: PySpark Connect")

try:
    from snowflake.snowpark import DataFrame as snowpark_dataframe  # type: ignore
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: Snowpark")

try:
    from duckdb import DuckDBPyConnection as duckdb_dataframe  # type: ignore
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: DuckDB")

try:
    from google.cloud import bigquery
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: BigQuery")

try:
    from daft import DataFrame as daft_dataframe
except (ModuleNotFoundError, ImportError):
    logger.debug("KO: BigQuery")


class CustomComputeException(Exception):
    pass


class CheckLevel(enum.Enum):
    """Level of verifications in cuallee"""

    WARNING = 0
    ERROR = 1


class CheckDataType(enum.Enum):
    """Accepted data types in checks"""

    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIMESTAMP = 4
    DUO = 5


class CheckStatus(enum.Enum):
    """Validation result criteria"""

    PASS = "PASS"
    FAIL = "FAIL"
    NO_RUN = "NO_RUN"


@dataclass
class Rule:
    """Predicate definition holder"""

    method: str
    column: Union[str, List[str], Tuple[str, str]]
    value: Optional[Any]
    data_type: CheckDataType
    coverage: float = 1.0
    options: Union[List[Tuple], None] = None
    status: Union[str, None] = None
    violations: int = 0
    pass_rate: float = 0.0
    ordinal: int = 0

    @property
    def settings(self) -> dict:
        """holds the additional settings for the predicate execution"""
        return dict(self.options)

    @property
    def key(self):
        """blake2s hash of the rule, made of method, column, value, options and coverage"""
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

    def evaluate_violations(self, result: Any, rows: int):
        """Calculates the row violations on the rule"""

        if isinstance(result, str):
            if result == "false":
                self.violations = rows
            elif result == "true":
                self.violations = 0
            else:
                self.violations = abs(int(result))
        elif isinstance(result, bool):
            if result is True:
                self.violations = 0
            elif result is False:
                self.violations = rows
        elif isinstance(result, int):
            if result == 0:
                self.violations = rows
            elif result < 0:
                self.violations = abs(result)
            elif (result > 0) and (result < rows):
                self.violations = rows - result

        else:
            self.violations = 0

    def evaluate_pass_rate(self, rows: int):
        """Percentage of successful rows by this rule"""
        if self.violations <= rows:
            try:
                self.pass_rate = 1 - (self.violations / rows)
            except ZeroDivisionError:
                self.pass_rate = 1.0
        else:
            try:
                self.pass_rate = rows / self.violations
            except ZeroDivisionError:
                self.pass_rate = 0.0

    def evaluate_status(self):
        """Overall PASS/FAIL status of the rule"""
        if self.pass_rate >= self.coverage:
            self.status = "PASS"
        else:
            self.status = "FAIL"

    def evaluate(self, result: Any, rows: int):
        """Generic rule evaluation for checks"""
        self.evaluate_violations(result, rows)
        self.evaluate_pass_rate(rows)
        self.evaluate_status()


class ComputeEngine(Protocol):
    """An interface for validatiosn to adhere to"""

    def compute(self, rules: Dict[str, Rule]) -> bool:
        """Returns compute instructions for each rule"""

    def validate_data_types(self, rules: List[Rule], dataframe: Any) -> bool:
        """Validates that all data types from checks match the dataframe with data"""

    def summary(self, check: Any, dataframe: Any) -> Any:
        """Computes all predicates and expressions for check summary"""


class Check:
    def __init__(
        self,
        level: Union[CheckLevel, int] = 0,
        name: str = "cuallee.check",
        *,
        execution_date: datetime = datetime.now(timezone.utc),
        table_name: str = None,
        session: Any = None,
    ):
        """
        A container of data quality rules.

        Args:
            level (CheckLevel): [0-1] value to describe if its a WARNING or ERROR check
            name (str): Normally the name of the dataset being verified, or a name for this check
            execution_date (date): An automatically generated timestamp of the check in UTC
            table_name (str): When using databases matches the table name of the source
            session (Session): When operating in Session enabled environments like Databricks or Snowflake

        """
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
        try:
            from .iso.checks import ISO
            from .bio.checks import BioChecks

            self.iso = ISO(self)
            self.bio = BioChecks(self)
        except (ModuleNotFoundError, ImportError) as err:
            logger.error(f"Dependency modules missing: {str(err)}")
        self.session = session

    def __repr__(self):
        standard = f"Check(level:{self.level}, desc:{self.name}, rules:{self.sum})"
        if self.table_name:
            standard += f" / table:{self.table_name}"
        return standard

    @property
    def sum(self):
        """Total number of rules in Check"""
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
        """
        Remove a key from rules and compute dictionaries

        Args:
            key (str): the blake2s key of the rule
        """
        if key in self._rule:
            self._rule.pop(key)

    def add_rule(self, method: str, *arg, **kwargs):
        """
        Add a new rule to the Check class.

        Args:
            method (str): Check name
            arg (list): Parameters of the check
        """
        return operator.methodcaller(method, *arg, **kwargs)(self)

    def delete_rule_by_key(self, keys: Union[str, List[str]]):
        """
        Delete rules from check based on keys.

        Args:
            keys (List[str]): a single or list of keys to remove from the check
        """
        if isinstance(keys, str):
            keys = [keys]

        [self._remove_rule_generic(key) for key in keys]
        return self

    def delete_rule_by_attribute(
        self,
        rule_attribute: Literal["method", "column", "coverage"],
        values: Union[List[str], List[float]],
    ):
        """
        Delete rule based on method(s) or column name(s) or coverage value(s).

        Args:
            rule_attribute (str): Finds a rule with by: method, column or coverage
            values (List[str]): Deletes a rule that matches the rule_attribute equal to the value in this parameter
        """
        if not isinstance(values, List):
            values = [values]

        _filter = lambda x: operator.attrgetter(rule_attribute)(x) in values

        [
            self._remove_rule_generic(key)
            for key in valfilter(_filter, self._rule).keys()
        ]
        return self

    def adjust_rule_coverage(self, rule_index: int, rule_coverage: float):
        """
        Adjust the ratio predicate/rows for a rule.
        It is intended to lower or increase tolerance without having to rewrite the entire check

        Args:
            rule_index (int): The position of the rule in the check list
            rule_coverage (float): New value between [0..1] for tolerance

        """
        target_rule = self.rules[rule_index]
        old_key = target_rule.key
        target_rule = self._rule.pop(old_key)
        target_rule.coverage = rule_coverage
        target_rule >> self._rule
        return self

    def is_complete(self, column: str, pct: float = 1.0):
        """
        Validation for non-null values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass

        """
        Rule("is_complete", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_empty(self, column: str, pct: float = 1.0):
        """
        Validation for null values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass

        """
        Rule("is_empty", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def are_complete(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """
        Validation for non-null values in a group of columns

        Args:
            column (List[str]): A tuple or list of column names in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("are_complete", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_unique(self, column: str, pct: float = 1.0, approximate: bool = False):
        """
        Validation for unique values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
            approximate (bool): A flag to speed up computation using an approximation through maximum relative std. dev.
        """
        (
            Rule(
                "is_unique",
                column,
                "N/A",
                CheckDataType.AGNOSTIC,
                pct,
                options={"approximate": approximate},
            )
            >> self._rule
        )
        return self

    def is_primary_key(self, column: str, pct: float = 1.0):
        """
        Validation for unique values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def are_unique(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """
        Validation for unique values in a group of columns

        Args:
            column (List[str]): A tuple or list of column names in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_composite_key(
        self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0
    ):
        """
        Validation for unique values in a group of columns

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("are_unique", column, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        Rule("is_greater_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def is_positive(self, column: str, pct: float = 1.0):
        """
        Validation for numeric greater than zero

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_than(column, 0, pct)

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater or equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule("is_greater_or_equal_than", column, value, CheckDataType.NUMERIC, pct)
            >> self._rule
        )
        return self

    def is_in_millions(self, column: str, pct: float = 1.0):
        """
        Validates that a column has values greater than 1M (1e6)

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_or_equal_than(column, 1e6, pct)

    def is_in_billions(self, column: str, pct: float = 1.0):
        """
        Validates that a column has values greater than 1B (1e9)

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_or_equal_than(column, 1e9, pct)

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        Rule("is_less_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def is_negative(self, column: str, pct: float = 1.0):
        """
        Validation for numeric less than zero

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_less_than(column, 0, pct)

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less or equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule("is_less_or_equal_than", column, value, CheckDataType.NUMERIC, pct)
            >> self._rule
        )
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric column equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        Rule("is_equal_than", column, value, CheckDataType.NUMERIC, pct) >> self._rule
        return self

    def has_pattern(self, column: str, value: str, pct: float = 1.0):
        """
        Validation for string type column matching regex expression

        Args:
            column (str): Column name in dataframe
            value (regex): A regular expression used to  match values in the `column`
            pct (float): The threshold percentage required to pass
        """
        Rule("has_pattern", column, value, CheckDataType.STRING, pct) >> self._rule
        return self

    def is_legit(self, column: str, pct: float = 1.0):
        """
        Validation for string columns giving wrong signal about completeness due to empty strings.

        Useful for reading CSV files and preventing empty strings being reported as valid records.
        This is an `alias` implementation of the `has_pattern` rule using `not black space` as the pattern
        Which validates the presence of non-empty characters between the begining and end of a string.

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("has_pattern", column, r"^\S+$", CheckDataType.STRING, pct) >> self._rule
        return self

    def has_min(self, column: str, value: float):
        """
        Validation of a column's minimum value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
        """
        Rule("has_min", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_max(self, column: str, value: float):
        """
        Validation of a column's maximum value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
        """
        Rule("has_max", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_std(self, column: str, value: float):
        """
        Validation of a column's standard deviation

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
        """
        Rule("has_std", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_mean(self, column: str, value: float):
        """
        Validation of a column's average/mean

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
        """
        Rule("has_mean", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def has_sum(self, column: str, value: float):
        """
        Validation of a sum of all values of a column

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
        """
        Rule("has_sum", column, value, CheckDataType.NUMERIC) >> self._rule
        return self

    def is_between(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """
        Validation of a column between a range

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        Rule("is_between", column, value, CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def not_contained_in(
        self,
        column: str,
        value: Union[List, Tuple],
        pct: float = 1.0,
    ):
        """
        Validation of column value not in set of given values

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule("not_contained_in", column, value, CheckDataType.AGNOSTIC, pct)
            >> self._rule
        )

        return self

    def not_in(self, column: str, value: Tuple[str, int, float], pct: float = 1.0):
        """
        Vaidation of column value not in set of given values

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        return self.not_contained_in(column, value, pct)

    def is_contained_in(
        self,
        column: str,
        value: Union[List, Tuple],
        pct: float = 1.0,
    ):
        """
        Validation of column value in set of given values

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """

        (
            Rule("is_contained_in", column, value, CheckDataType.AGNOSTIC, pct)
            >> self._rule
        )

        return self

    def is_in(self, column: str, value: Tuple[str, int, float], pct: float = 1.0):
        """
        Vaidation of column value in set of given values

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        return self.is_contained_in(column, value, pct)

    def is_t_minus_n(self, column: str, value: int, pct: float = 1.0):
        """
        Validate that date is `n` days before the current date

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The number of days before the current date
            pct (float): The threshold percentage required to pass
        """
        yesterday = datetime.utcnow() - timedelta(days=value)
        return self.is_in(column, tuple([yesterday.strftime("%Y-%m-%d")]), pct)

    def is_t_minus_1(self, column: str, pct: float = 1.0):
        """
        Validate that date is yesterday

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 1, pct)

    def is_t_minus_2(self, column: str, pct: float = 1.0):
        """
        Validate that date is 2 days ago

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 2, pct)

    def is_t_minus_3(self, column: str, pct: float = 1.0):
        """
        Validate that date is 3 days ago

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 3, pct)

    def is_yesterday(self, column: str, pct: float = 1.0):
        """
        Validate that date is yesterday

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_1(column, pct)

    def is_today(self, column: str, pct: float = 1.0):
        """
        Validate that date is today

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 0, pct)

    def has_percentile(
        self, column: str, value: float, percentile: float, precision: int = 10000
    ):
        """
        Validation of a column percentile value using approximantion

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            percentile (float): Value between [0..1] i.e. `0.5` for median
            precision (float): The precision to calculate percentiles

        """
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
        """
        Validates a number resides inside the quartile(1) and quartile(3) of the range of values

        Args:
            column (str): Column name in dataframe
            value (List[number]): A number between 0 and 1 demarking the quartile
            pct (float): The threshold percentage required to pass
        """
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
        """
        Validation the correspondance of a column value based on another column maximum

        Args:
            column_source (str): Column used to obtain the row with the max value
            column_target (str): Column used to varify the matching value
            value (str,number): The value to match against
        """
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
        """
        Validation the correspondance of a column value based on another column minimum

        Args:
            column_source (str): Column used to obtain the row with the min value
            column_target (str): Column used to varify the matching value
            value (str,number): The value to match against
        """
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
        """
        Validates the correlation in a range of [0..1] between 2 columns

        Args:
            column_left (str): Column name in dataframe
            column_right (str): Column name in dataframe
            value (float): Value to match the correlation
        """
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
        """
        Validation of a column satisfying a SQL-like predicate

        Args:
            column (str): Column name in the dataframe
            predicate (str): A predicate written in SQL-like syntax
            pct (float): The threshold percentage required to pass
        """
        Rule("satisfies", column, predicate, CheckDataType.AGNOSTIC, pct) >> self._rule
        return self

    def has_cardinality(self, column: str, value: int):
        """
        Validates the number of distinct values in a column

        Args:
            column (str): Column name in the dataframe
            value (int): The number of expected distinct values on a column
        """
        Rule("has_cardinality", column, value, CheckDataType.AGNOSTIC) >> self._rule
        return self

    def has_infogain(self, column: str, pct: float = 1.0):
        """
        Validate cardinality > 1.
        Particularly useful when validating categorical data for Machine Learning

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass

        """
        (
            Rule(
                method="has_infogain",
                column=column,
                value="N/A",
                data_type=CheckDataType.AGNOSTIC,
                coverage=pct,
            )
            >> self._rule
        )
        return self

    def has_entropy(self, column: str, value: float, tolerance: float = 0.01):
        """
        Validation for entropy calculation on continuous variables/features on `log2`.
        Useful in Machine Learning classifications to test imbalanced datasets with low entropy.

        Args:
            column (str): Column name in the dataframe
            value (float): The expected entropy value
            tolerance (float): The tolerance/precision used when comparing the actual and expected value

        Examples:

        """
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
        """
        Validates a datetime column is in a Mon-Fri time range

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_weekday", column, "Mon-Fri", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_weekend(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is in a Sat-Sun time range

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_weekend", column, "Sat-Sun", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_monday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Monday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_monday", column, "Mon", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_tuesday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Tuesday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_tuesday", column, "Tue", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_wednesday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Wednesday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_wednesday", column, "Wed", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_thursday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Thursday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_thursday", column, "Thu", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_friday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Friday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_friday", column, "Fri", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_saturday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Saturday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_saturday", column, "Sat", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_sunday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Sunday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("is_on_sunday", column, "Sun", CheckDataType.DATE, pct) >> self._rule
        return self

    def is_on_schedule(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """
        Validation of a datetime column between an hour interval

        Args:
            column (str): Column name in the dataframe
            value (Tuple[int,int]): A tuple indicating a 24hr day interval. i.e. (9,17) for 9am to 5pm
            pct (float): The threshold percentage required to pass
        """
        (
            Rule("is_on_schedule", column, value, CheckDataType.TIMESTAMP, pct)
            >> self._rule
        )
        return self

    def is_daily(
        self, column: str, value: Union[None, List[int]] = None, pct: float = 1.0
    ):
        """
        Validates that there is no missing dates using only week days in the date/timestamp column.

        An alternative day combination can be provided given that a user wants to validate only certain dates.
        For example in PySpark to validate that time series are every Wednesday consecutively on a year
        without any missing values, the value input should contain `[4]` as it represent the numeric
        equivalence of the day of week Wednesday.

        Args:
            column (str): Column name in the dataframe
            value (List[int]): A list of numbers describing the days of the week to consider. i.e. Pyspark uses [2, 3, 4, 5, 6] for Mon-Fri
            pct (float): The threshold percentage required to pass
        """
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
        """
        Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation.

        Args:
            column_group (str): The dataframe column used to group events
            column_event (str): The state of the event within the group
            column_order (List[date,number,str]): The order within the group, should be deterministic and without collisions.
            edges (List[Tuple[str,str]]): The combinations of events expected in the data frame i.e `[("A","B"), ("B","C")]`


        ???+ example "Example"

            Given the following fictitious dataset example:

            | date       | ticket   | status      |
            |------------|----------|-------------|
            | 2024-01-01 | CASE-001 | New         |
            | 2024-01-02 | CASE-001 | In Progress |
            | 2024-01-03 | CASE-001 | Closed      |

            You can validate that events for each ticket follow certain sequence by using:

            ``` python
            from cuallee import Check, CheckLevel
            df = spark.createDataFrame(
                 [
                     ["2024-01-01", "CASE-001", "New"],
                     ["2024-01-02", "CASE-001", "In Progress"],
                     ["2024-01-03", "CASE-001", "Closed"],
                 ],
                 ["date", "ticket", "status"],
             )


            check = Check(CheckLevel.WARNING, "WorkflowValidation")
            check.has_workflow(
                column_group="ticket",
                column_event="status",
                column_order="date",
                edges=[(None, "New"),("New", "In Progress"),("In Progress","Closed"), ("Closed", None)]
            )

            # Validate
            check.validate(df).show(truncate=False)

            # Result
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+
            |id |timestamp          |check             |level  |column                      |rule        |value                                                                               |rows|violations|pass_rate|pass_threshold|status|
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+
            |1  |2024-05-11 11:24:00|WorkflowValidation|WARNING|('ticket', 'status', 'date')|has_workflow|((None, 'New'), ('New', 'In Progress'), ('In Progress', 'Closed'), ('Closed', None))|3   |0         |1.0      |1.0           |PASS  |
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+

            ```

        The check validates that:

        - Nothing preceds a `New` state
        - `In Progress` follows the `New` event
        - `Closed` follows the `In Progress` event
        - Nothing follows after `Closed` state

        """
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

    def is_custom(
        self, column: Union[str, List[str]], fn: Callable = None, pct: float = 1.0
    ):
        """
        Uses a user-defined function that receives the to-be-validated dataframe
        and uses the last column of the transformed dataframe to summarize the check

        Args:
            column (str): Column(s) required for custom function
            fn (Callable): A function that receives a dataframe as input and returns a dataframe with at least 1 column as result
            pct (float): The threshold percentage required to pass
        """

        (Rule("is_custom", column, fn, CheckDataType.AGNOSTIC, pct) >> self._rule)
        return self

    def validate(self, dataframe: Any):
        """
        Compute all rules in this check for specific data frame

        Args:
            dataframe (Union[pyspark,snowpark,pandas,polars,duckdb,bigquery]): A dataframe object
        """

        # Stop execution if the there is no rules in the check
        assert not self.empty, "Check is empty. Try adding some rules?"

        # When dataframe is PySpark DataFrame API
        if "pyspark_dataframe" in globals() and isinstance(
            dataframe, pyspark_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.pyspark_validation")

        elif "pyspark_connect_dataframe" in globals() and isinstance(
            dataframe, pyspark_connect_dataframe
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

        elif "bigquery" in globals() and isinstance(dataframe, bigquery.table.Table):
            self.compute_engine = importlib.import_module("cuallee.bigquery_validation")

        elif "polars_dataframe" in globals() and isinstance(
            dataframe, polars_dataframe
        ):
            self.compute_engine = importlib.import_module("cuallee.polars_validation")

        elif "daft_dataframe" in globals() and isinstance(dataframe, daft_dataframe):
            self.compute_engine = importlib.import_module("cuallee.daft_validation")

        else:
            raise Exception(
                "Cuallee is not ready for this data structure. You can log a Feature Request in Github."
            )

        assert self.compute_engine.validate_data_types(
            self.rules, dataframe
        ), "Invalid data types between rules and dataframe"

        return self.compute_engine.summary(self, dataframe)


class Control:
    @staticmethod
    def completeness(dataframe, **kwargs):
        """Control of null values on data frames"""
        check = Check(CheckLevel.WARNING, name="Completeness", **kwargs)
        [check.is_complete(c) for c in dataframe.columns]
        return check.validate(dataframe)

    @staticmethod
    def information(dataframe, **kwargs):
        """Information gain"""
        check = Check(CheckLevel.WARNING, name="Information", **kwargs)
        [check.is_complete(c) for c in dataframe.columns]
        [check.has_infogain(c) for c in dataframe.columns]
        [check.is_legit(c) for c in dataframe.columns]
        return check.validate(dataframe)

    @staticmethod
    def percentage_fill(dataframe, **kwargs):
        """Control the percentage of values filled"""

        compute = compose(
            map_curried(operator.attrgetter("pass_rate")),
            operator.methodcaller("collect"),
            operator.methodcaller("select", "pass_rate"),
        )
        result = list(compute(Control.completeness(dataframe, **kwargs)))
        return sum(result) / len(result)

    @staticmethod
    def percentage_empty(dataframe, **kwargs):
        """Control the percentage of values empty"""
        return 1 - Control.percentage_fill(dataframe, **kwargs)

    @staticmethod
    def intelligence(dataframe, **kwargs) -> List[str]:
        """Return worthy columns"""
        complete_gain = compose(
            list,
            map_curried(lambda x: x.column),
            operator.methodcaller("collect"),
            operator.methodcaller("distinct"),
            operator.methodcaller("select", "column"),
            operator.methodcaller("where", "count == 3"),
            operator.methodcaller("count"),
            operator.methodcaller("groupby", "column"),
            operator.methodcaller("where", "status == 'PASS'"),
        )
        return complete_gain(Control.information(dataframe))
