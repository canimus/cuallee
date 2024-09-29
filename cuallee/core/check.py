import enum
import importlib
import operator
import re
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, Union

from toolz import first, valfilter

from ..family.generic import GenericCheck
from ..family.numeric import NumericCheck
from ..family.stats import StatsCheck
from ..family.string import StringCheck


class CheckLevel(enum.Enum):
    """Level of verifications in cuallee"""

    WARNING = 0
    WARN = 0
    ERROR = 1
    ERR = 1


class CheckStatus(enum.Enum):
    """Validation result criteria"""

    PASS = "PASS"
    FAIL = "FAIL"
    NO_RUN = "NO_RUN"


class Check(GenericCheck, NumericCheck, StringCheck, StatsCheck):
    def __init__(
        self,
        level: Union[CheckLevel, int] = 0,
        name: str = "cuallee.check",
        *,
        execution_date: datetime = datetime.now(timezone.utc),
        table_name: str = None,
        session: Any = None,
        config: Dict = {},
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
        self._rule: Dict = {}
        self.compute_engine: ModuleType

        if isinstance(level, int):
            # When the user is lazy and wants to do WARN=0, or ERR=1
            level = CheckLevel(level)

        self.level = level
        self.name = name
        self.date = execution_date
        self.rows = -1
        self.config = config
        self.table_name = table_name
        self.dtype = "cuallee.dataframe"
        self.session = session

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

    def __repr__(self):
        _attrs = valfilter(
            lambda x: x is not None,
            {
                "level": self.level.name,
                "name": self.name,
                "rules": self.sum,
                "table": self.table_name,
                "config": self.config,
            },
        )

        return f"Check{_attrs}"

    def add_rule(self, method: str, *args, **kwargs):
        """
        Add a new rule to the Check class.

        Args:
            method (str): Check name
            args (list): Parameters of the Rule
            kwargs (dict): Dictionary of options for the Rule
        """
        return operator.methodcaller(method, *args, **kwargs)(self)

    def validate(self, dataframe: Any, ok: bool = False):
        """
        Compute all rules in this check for specific data frame

        Args:
            dataframe (Union[pyspark,snowpark,pandas,polars,duckdb,bigquery]): A dataframe object
        """

        # Stop execution if the there is no rules in the check
        assert not self.empty, "Check is empty. Try adding some rules?"

        self.dtype = first(re.match(r".*'(.*)'", str(type(dataframe))).groups())
        match self.dtype:
            case self.dtype if "pyspark" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.core.engines.pyspark_qa"
                )
            case self.dtype if "pandas" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.core.engines.pandas_qa"
                )
            case self.dtype if "snowpark" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.snowpark_validation"
                )
            case self.dtype if "polars" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.polars_validation"
                )
            case self.dtype if "duckdb" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.duckdb_validation"
                )
            case self.dtype if "bigquery" in self.dtype:
                self.compute_engine = importlib.import_module(
                    "cuallee.bigquery_validation"
                )
            case self.dtype if "daft" in self.dtype:
                self.compute_engine = importlib.import_module("cuallee.daft_validation")
            case _:
                raise NotImplementedError(
                    f"{self.dtype} is not yet implemented in cuallee"
                )

        assert self.compute_engine.validate_data_types(
            self.rules, dataframe
        ), "Invalid data types between rules and dataframe"

        if ok:
            result = self.compute_engine.ok(self, dataframe)
        else:
            result = self.compute_engine.summary(self, dataframe)
        return result
