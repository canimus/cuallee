import re
import operator
from itertools import repeat
from numbers import Number
from typing import Dict, List, Union

import numpy as np
import polars as pl  # type: ignore
from toolz import compose, first  # type: ignore

from cuallee import Check, Rule
from cuallee import utils as cuallee_utils


class Compute:
    @staticmethod
    def _result(series: pl.Series) -> int:
        """It retrieves the sum result of the polar predicate"""
        return compose(operator.itemgetter(0))(series)

    def is_complete(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate not null"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).is_not_null().cast(pl.Int8))
            .sum()
            .to_series()
        )

    def are_complete(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate absence of null in group of columns"""
        return Compute._result(
            dataframe.select(
                [pl.col(c).is_not_null().cast(pl.Int8).sum() for c in rule.column]
            ).sum(axis=1)
            / len(rule.column)
        )

    def is_unique(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate absence of duplicates"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).is_unique().cast(pl.Int8)).sum()
        )

    def are_unique(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate absence of duplicate in group of columns"""
        return Compute._result(
            dataframe.select(
                [pl.col(c).is_unique().cast(pl.Int8).sum() for c in rule.column]
            ).sum(axis=1)
            / len(rule.column)
        )

    def is_greater_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate column values greater than threshold"""
        return Compute._result(
            dataframe.select(
                operator.gt(pl.col(rule.column), rule.value).cast(pl.Int8)
            ).sum()
        )

    def is_greater_or_equal_than(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, int]:
        """Validates column values greater or equal than threshold"""
        return Compute._result(
            dataframe.select(
                operator.ge(pl.col(rule.column), rule.value).cast(pl.Int8)
            ).sum()
        )

    def is_less_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validates column values less than threshold"""
        return Compute._result(
            dataframe.select(
                operator.lt(pl.col(rule.column), rule.value).cast(pl.Int8)
            ).sum()
        )

    def is_less_or_equal_than(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, int]:
        """Validates column values less or equal than threshold"""
        return Compute._result(
            dataframe.select(
                operator.le(pl.col(rule.column), rule.value).cast(pl.Int8)
            ).sum()
        )

    def is_equal_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validates column equality with threshold"""
        return Compute._result(
            dataframe.select(
                operator.eq(pl.col(rule.column), rule.value).cast(pl.Int8)
            ).sum()
        )

    def has_pattern(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Conformance on column values to regular expression threshold"""
        return Compute._result(
            dataframe.select(
                operator.gt(pl.col(rule.column).str.count_match(rule.value), 0).cast(
                    pl.Int8
                )
            )
            .sum()
            .to_series()
        )

    def has_min(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate minimum value on column"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).min() == rule.value).to_series()
        )

    def has_max(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate maximum value on column"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).max() == rule.value).to_series()
        )

    def has_std(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate standard deviation on column"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).std() == rule.value).to_series()
        )

    def has_mean(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate mean value on column"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).mean() == rule.value).to_series()
        )

    def has_sum(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate sum value on column"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).sum() == rule.value).to_series()
        )

    def is_between(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate value inclusion on threshold boundaries"""
        low, high = rule.value
        return Compute._result(
            dataframe.select(
                pl.col(rule.column).is_between(low, high, include_bounds=True).cast(pl.Int8)
            ).sum()
        )


    def is_contained_in(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate set inclusion"""
        return Compute._result(
            dataframe.select(
                pl.col(rule.column).is_in(rule.value).cast(pl.Int8)
            ).sum()
        )

    def has_percentile(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Percentile range verification for column"""
        return (
            np.percentile(
                dataframe.select(pl.col(rule.column)).to_numpy(), rule.settings["percentile"] * 100
            )  # type: ignore
            == rule.value  # type: ignore
        )

    def has_max_by(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Adjacent column maximum value verifiation on threshold"""
        base, target = rule.column
        return Compute._result(
            dataframe.filter(pl.col(base) == pl.col(base).max()).select(pl.col(target) == rule.value).to_series()
        )

    def has_min_by(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Adjacent column minimum value verifiation on threshold"""
        base, target = rule.column
        return Compute._result(
            dataframe.filter(pl.col(base) == pl.col(base).min()).select(pl.col(target) == rule.value).to_series()
        )

    def has_correlation(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        col_a, col_b = rule.column
        return Compute._result(
            dataframe.select(pl.col(col_a), pl.col(col_b))
            .pearson_corr()
            .fill_nan(0)
            .fill_null(0)
            .select(pl.col(col_b))
            .head(1)
            .to_series()
        )

    def satisfies(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.eval(rule.value).astype(int).sum()

    def has_entropy(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
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

    def is_on_weekday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.dayofweek.between(0, 4).astype(int).sum()
        )

    def is_on_weekend(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.dayofweek.between(5, 6).astype(int).sum()
        )

    def is_on_monday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(0).astype(int).sum()

    def is_on_tuesday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(1).astype(int).sum()

    def is_on_wednesday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(2).astype(int).sum()

    def is_on_thursday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(3).astype(int).sum()

    def is_on_friday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(4).astype(int).sum()

    def is_on_saturday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(5).astype(int).sum()

    def is_on_sunday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return dataframe.loc[:, rule.column].dt.dayofweek.eq(6).astype(int).sum()

    def is_on_schedule(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        return (
            dataframe.loc[:, rule.column].dt.hour.between(*rule.value).astype(int).sum()
        )

    def is_daily(self, rule: Rule, dataframe: pl.DataFrame) -> complex:
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
        sequence = (
            sequence[sequence.ts.dt.dayofweek.isin(day_mask)]
            .reset_index(drop=True)
            .ts.unique()
            .astype(np.datetime64)
        )

        delivery = (
            dataframe[dataframe[rule.column].dt.dayofweek.isin(day_mask)][rule.column]
            .dt.date.astype(np.datetime64)
            .values
        )

        # No difference between sequence of daily as a complex number
        return complex(len(dataframe), len(set(sequence).difference(delivery)))

    def is_inside_interquartile_range(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, complex]:
        lower, upper = dataframe[rule.column].quantile(rule.value).values
        return dataframe[rule.column].between(lower, upper).astype(int).sum()

    def has_workflow(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
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
    """Polars computes directly on the predicates"""
    return True


def validate_data_types(rules: List[Rule], dataframe: pl.DataFrame):
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""

    # # COLUMNS
    # # =======
    # rule_match = cuallee_utils.match_columns(rules, dataframe.columns)
    # assert not rule_match, f"Column(s): {rule_match} are not present in dataframe"

    # # NUMERIC
    # # =======
    # numeric_columns = cuallee_utils.get_rule_columns(
    #     cuallee_utils.get_numeric_rules(rules)
    # )
    # numeric_dtypes = dataframe.select_dtypes("number")
    # numeric_match = cuallee_utils.match_data_types(numeric_columns, numeric_dtypes)
    # assert not numeric_match, f"Column(s): {numeric_match} are not numeric"

    # # DATE
    # # =======
    # date_columns = cuallee_utils.get_rule_columns(cuallee_utils.get_date_rules(rules))
    # date_dtypes = dataframe.select_dtypes("datetime")
    # date_match = cuallee_utils.match_data_types(date_columns, date_dtypes)
    # assert not date_match, f"Column(s): {date_match} are not date"

    # # TIMESTAMP
    # # =======
    # timestamp_columns = cuallee_utils.get_rule_columns(
    #     cuallee_utils.get_timestamp_rules(rules)
    # )
    # timestamp_dtypes = dataframe.select_dtypes("datetime64")
    # timestamp_match = cuallee_utils.match_data_types(
    #     timestamp_columns, timestamp_dtypes
    # )
    # assert not timestamp_match, f"Column(s): {timestamp_match} are not timestamp"

    # # STRING
    # # =======
    # string_columns = cuallee_utils.get_rule_columns(
    #     cuallee_utils.get_string_rules(rules)
    # )
    # string_dtypes = dataframe.select_dtypes("object")
    # string_match = cuallee_utils.match_data_types(string_columns, string_dtypes)
    # assert not string_match, f"Column(s): {string_match} are not string"

    return True


def summary(check: Check, dataframe: pl.DataFrame):
    compute = Compute()
    unified_results = {
        rule.key: [operator.methodcaller(rule.method, rule, dataframe)(compute)]
        for rule in check.rules
    }

    def _calculate_violations(result, nrows):
        if isinstance(result, pl.DataFrame):
            result = first(result.row(0))

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
        if isinstance(result, pl.DataFrame):
            result = first(result.row(0))

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
            "column": str(rule.column),
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
    pl.Config.set_tbl_cols(12)
    return pl.DataFrame(computation_basis)
