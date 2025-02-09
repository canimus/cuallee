import enum
import hashlib
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from toolz import valfilter  # type: ignore


class RuleDataType(enum.Enum):
    """Accepted data types in checks"""

    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIMESTAMP = 4
    ARRAY = 5
    MAP = 6


@dataclass
class Rule:
    """Predicate definition holder"""

    method: str
    column: Union[str, List[str], Tuple[str, str]]
    value: Optional[Any]
    data_type: RuleDataType
    coverage: float = 1.0
    options: Union[List[Tuple], Iterable[tuple[Any, Any]], None] = None
    status: Union[str, None] = None
    violations: int = 0
    pass_rate: float = 0.0
    ordinal: int = 0
    name: Union[str, None] = None

    @property
    def settings(self) -> dict:
        """holds the additional settings for the predicate execution"""
        return dict(self.options)  # type: ignore

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

        if isinstance(self.value, Tuple) & (self.data_type == RuleDataType.AGNOSTIC):
            # All values can only be of one data type in a rule
            if len(Counter(map(type, self.value)).keys()) > 1:
                raise ValueError("Data types in rule values are inconsistent")

        if (
            self.options
            and isinstance(self.options, dict)
            and (rule_name := self.options.get("name"))
        ):
            self.name = rule_name
        else:
            self.name = self.method

        if isinstance(self.data_type, int):
            self.data_type = RuleDataType(self.data_type)

    def __repr__(self):
        _attrs = valfilter(
            lambda x: x is not None,
            {
                "ordinal": self.ordinal,
                "method": self.name,
                "column": self.column,
                "value": self.value,
                "data_type": self.data_type.name,
                "coverage": self.coverage,
                "options": self.options,
            },
        )

        return f"Rule{_attrs}"

    def __rshift__(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        ordinal = len(rule_dict.keys())
        self.ordinal = ordinal + 1
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

    def format_value(self):
        """Removes verbosity for Callable values"""
        if isinstance(self.value, Callable):
            if self.options and isinstance(self.options, dict):
                return self.options.get("custom_value", "f(x)")
        else:
            return str(self.value)
