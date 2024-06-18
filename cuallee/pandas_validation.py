from typing import Dict, Union, List
from cuallee import Check, Rule
import pandas as pd  # type: ignore
import operator
import numpy as np
import re
from toolz import first  # type: ignore
from numbers import Number
from cuallee import utils as cuallee_utils
from itertools import repeat


class Compute:
    def is_complete(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].notnull().sum()

    def is_empty(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].isnull().sum()

    def are_complete(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].notnull().astype(int).sum().sum() / len(
            rule.column
        )

    def is_unique(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].nunique()

    def are_unique(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].nunique().sum() / len(rule.column)

    def is_greater_than(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].gt(rule.value).sum()

    def is_greater_or_equal_than(
        self, rule: Rule, dataframe: pd.DataFrame
    ) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].ge(rule.value).sum()

    def is_less_than(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].lt(rule.value).sum()

    def is_less_or_equal_than(
        self, rule: Rule, dataframe: pd.DataFrame
    ) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].le(rule.value).sum()

    def is_equal_than(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].eq(rule.value).sum()

    def has_pattern(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column]
            .fillna("")
            .str.match(re.compile(rule.value))  # type: ignore
            .astype(int)
            .sum()
        )

    def has_min(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].min() == rule.value

    def has_max(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].max() == rule.value

    def has_std(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].std() == rule.value

    def has_mean(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].mean() == rule.value

    def has_sum(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].sum() == rule.value

    def has_cardinality(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].nunique() == rule.value

    def has_infogain(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].nunique() > 1

    def is_between(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].between(*rule.value).astype(int).sum()

    def is_contained_in(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].isin(rule.value).astype(int).sum()

    def not_contained_in(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            (dataframe.loc[:, rule.column].isin(rule.value).eq(False)).astype(int).sum()
        )

    def has_percentile(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            np.percentile(dataframe.loc[:, rule.column].values, rule.settings["percentile"] * 100)  # type: ignore
            == rule.value  # type: ignore
        )

    def has_max_by(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[dataframe.loc[:, rule.column[1]].idxmax(), rule.column[0]]
            == rule.value
        )

    def has_min_by(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[dataframe.loc[:, rule.column[1]].idxmin(), rule.column[0]]
            == rule.value
        )

    def has_correlation(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, (rule.column[0], rule.column[1])]
            .corr()
            .fillna(0)
            .iloc[0, 1]
            == rule.value
        )

    def satisfies(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.eval(rule.value).astype(int).sum()

    def has_entropy(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        def entropy(labels):
            """Computes entropy of 0-1 vector."""
            n_labels = len(labels)

            if n_labels <= 1:
                return 0

            counts = np.bincount(labels)
            probs = counts[np.nonzero(counts)] / n_labels
            n_classes = len(probs)

            if n_classes <= 1:
                return 0

            return -np.sum(probs * np.log(probs)) / np.log(n_classes)

        return entropy(dataframe.loc[:, rule.column].values) == float(rule.value)

    def is_on_weekday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.dayofweek.between(0, 4).astype(int).sum()
        )

    def is_on_weekend(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.dayofweek.between(5, 6).astype(int).sum()
        )

    def is_on_monday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(0).astype(int).sum()

    def is_on_tuesday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(1).astype(int).sum()

    def is_on_wednesday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(2).astype(int).sum()

    def is_on_thursday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(3).astype(int).sum()

    def is_on_friday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(4).astype(int).sum()

    def is_on_saturday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(5).astype(int).sum()

    def is_on_sunday(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(6).astype(int).sum()

    def is_on_schedule(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.hour.between(*rule.value).astype(int).sum()
        )

    def is_daily(self, rule: Rule, dataframe: pd.DataFrame) -> complex:
        if rule.value is None:
            day_mask = [0, 1, 2, 3, 4]
        else:
            day_mask = rule.value

        lower, upper = (
            dataframe.loc[:, rule.column].agg([np.min, np.max]).dt.strftime("%Y-%m-%d")
        )
        sequence = (
            pd.date_range(start=lower, end=upper, freq="D").rename("ts").to_frame()
        )
        sequence = list(
            sequence[sequence.ts.dt.dayofweek.isin(day_mask)]
            .reset_index(drop=True)
            .ts.unique()
            .astype("datetime64[ms]")
        )

        delivery = list(
            dataframe[dataframe[rule.column].dt.dayofweek.isin(day_mask)][
                rule.column
            ].dt.date.astype("datetime64[ms]")
        )

        # No difference between sequence of daily as a complex number
        return complex(len(dataframe), len(set(sequence).difference(delivery)))

    def is_inside_interquartile_range(
        self, rule: Rule, dataframe: pd.DataFrame
    ) -> Union[bool, complex]:
        lower, upper = dataframe[rule.column].quantile(rule.value).values
        return dataframe[rule.column].between(lower, upper).astype(int).sum()

    def has_workflow(self, rule: Rule, dataframe: pd.DataFrame) -> Union[bool, int]:
        """Compliance with adjacency matrix"""

        def workflow(dataframe):
            group, event, order = rule.column
            CUALLEE_EVENT = "cuallee_event"
            CUALLEE_EDGE = "cuallee_edge"
            CUALLEE_GRAPH = "cuallee_graph"
            dataframe[CUALLEE_EVENT] = (
                dataframe.loc[:, rule.column]
                .sort_values(by=[group, order], ascending=True)
                .groupby([group])[event]
                .shift(-1)
                .replace(np.nan, None)
            )
            dataframe[CUALLEE_EDGE] = dataframe[[event, CUALLEE_EVENT]].apply(
                lambda x: (x[event], x[CUALLEE_EVENT]), axis=1
            )
            dataframe[CUALLEE_GRAPH] = list(repeat(rule.value, len(dataframe)))

            return (
                dataframe.apply(lambda x: x[CUALLEE_EDGE] in x[CUALLEE_GRAPH], axis=1)
                .astype("int")
                .sum()
            )

        return workflow(dataframe.loc[:, rule.column])


def compute(rules: Dict[str, Rule]):
    """Pandas computes directly on the predicates"""
    return True


def validate_data_types(rules: List[Rule], dataframe: pd.DataFrame):
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
    numeric_dtypes = dataframe.select_dtypes("number")
    numeric_match = cuallee_utils.match_data_types(numeric_columns, numeric_dtypes)
    assert not numeric_match, f"Column(s): {numeric_match} are not numeric"

    # DATE
    # =======
    date_columns = cuallee_utils.get_rule_columns(cuallee_utils.get_date_rules(rules))
    date_dtypes = dataframe.select_dtypes("datetime")
    date_match = cuallee_utils.match_data_types(date_columns, date_dtypes)
    assert not date_match, f"Column(s): {date_match} are not date"

    # TIMESTAMP
    # =======
    timestamp_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_timestamp_rules(rules)
    )
    timestamp_dtypes = dataframe.select_dtypes("datetime64")
    timestamp_match = cuallee_utils.match_data_types(
        timestamp_columns, timestamp_dtypes
    )
    assert not timestamp_match, f"Column(s): {timestamp_match} are not timestamp"

    # STRING
    # =======
    string_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_string_rules(rules)
    )
    string_dtypes = dataframe.select_dtypes("object")
    string_match = cuallee_utils.match_data_types(string_columns, string_dtypes)
    assert not string_match, f"Column(s): {string_match} are not string"

    return True


def summary(check: Check, dataframe: pd.DataFrame):
    compute = Compute()
    unified_results = {
        rule.key: [operator.methodcaller(rule.method, rule, dataframe)(compute)]
        for rule in check.rules
    }

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            if isinstance(result, complex):
                return result.imag
            else:
                return nrows - result

    def _calculate_pass_rate(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 1.0
            else:
                return 0.0
        elif isinstance(result, Number):
            if isinstance(result, complex):
                if result.imag > 0:
                    if result.imag > nrows:
                        return nrows / result.imag
                    else:
                        return result.imag / nrows
                else:
                    return 1.0

            else:
                return result / nrows

    def _evaluate_status(pass_rate, pass_threshold):
        if pass_rate >= pass_threshold:
            return "PASS"

        return "FAIL"

    rows = len(dataframe)

    computation_basis = [
        {
            "id": index,
            "timestamp": check.date.strftime("%Y-%m-%d %H:%M:%S"),
            "check": check.name,
            "level": check.level.name,
            "column": rule.column,
            "rule": rule.method,
            "value": rule.value,
            "rows": rows,
            "violations": _calculate_violations(first(unified_results[hash_key]), rows),
            "pass_rate": _calculate_pass_rate(first(unified_results[hash_key]), rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(first(unified_results[hash_key]), rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    return pd.DataFrame(computation_basis)
