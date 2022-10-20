import enum
import operator
import snowflake.snowpark.functions as F  # type: ignore
import snowflake.snowpark.types as T  # type: ignore

from typing import Union, Dict, Collection, Type, Callable, Optional, Any, Tuple
from dataclasses import dataclass
from snowflake.snowpark import DataFrame, Column  
from toolz import valfilter  # type: ignore

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

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
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
    return set(map(str.upper,
        cuallee_utils.get_column_set(map(columns, valfilter(filter, rules).values()))  # type: ignore
    )).difference(fn(dataframe))


def numeric_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T._NumericType)


def string_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, (T.DateType, T.TimestampType))
    # set(
    #    [f.name for f in dataframe.schema.fields if isinstance(f.datatype, T.DateType) or isinstance(f.datatype, T.TimestampType) or isinstance(f.datatype, T.TimeType)]  # type: ignore
    # )  # TODO: Check with Herminio: T.TimestampNTZType does not exist. However there is a TimeType --> to use for TimeStamp?.


def timestamp_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, (T.TimestampType, T.TimeType))


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


def summary(check: Check, dataframe: DataFrame):
    """Compute all rules in this check for specific data frame"""
    return "I am a Snow DataFrame!"
