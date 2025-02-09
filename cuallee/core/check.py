import enum
import importlib
import operator
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from toolz import first, valfilter  # type: ignore

from ..family.generic import GenericCheck
from ..family.numeric import NumericCheck
from ..family.stats import StatsCheck
from ..family.string import StringCheck


class CheckLevel(enum.Enum):
    """Level of verifications in cuallee"""

    WARNING = WARN = 0
    ERROR = ERR = 1


class CheckStatus(enum.Enum):
    """Validation result criteria"""

    PASS = "PASS"
    FAIL = "FAIL"
    NO_RUN = "NO_RUN"


ENGINES = ["pyspark", "pandas", "snowpark", "polars", "duckdb", "bigquery", "daft"]


class Check(GenericCheck, NumericCheck, StringCheck, StatsCheck):
    def __init__(
        self,
        level: Union[CheckLevel, int] = 0,
        name: str = "cuallee.check",
        *,
        execution_date: datetime = datetime.now(timezone.utc),
        table_name: Union[str, None] = None,
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
        self._rule: Dict[str, str] = {}
        self.compute_engine: Union[Any, None] = None
        self.level = CheckLevel(level) if isinstance(level, int) else level
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
        return len(self._rule)

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
        attrs = valfilter(
            None.__ne__,
            {
                "level": self.level.name,
                "name": self.name,
                "rules": self.sum,
                "table": self.table_name,
                "config": self.config,
            },
        )
        return f"Check{attrs}"

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
            ok (bool): A boolean flag to return a boolean instead of a dataframe with the outcome of the check
        """

        # Stop execution if the there is no rules in the check
        assert not self.empty, "Check is empty. Try adding some rules?"

        frame_engine: Optional[re.Match] = re.match(r".*'(.*)'", str(type(dataframe)))
        if frame_engine:
            frame_name: Optional[tuple[str]] = frame_engine.groups()
            if frame_name:
                self.dtype = first(frame_name)
        engine_key: Optional[str] = next((k for k in ENGINES if k in self.dtype), None)

        if not engine_key:
            raise NotImplementedError(f"{self.dtype} is not yet implemented in cuallee")

        self.compute_engine = importlib.import_module(f"cuallee.engine.{engine_key}")

        assert self.compute_engine.dtypes(
            self.rules, dataframe
        ), "Invalid data types between rules and dataframe"

        return (
            self.compute_engine.ok(self, dataframe)
            if ok
            else self.compute_engine.summary(self, dataframe)
        )
