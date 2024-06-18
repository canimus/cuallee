import daft
import operator
import statistics
import numpy as np
import pandas as pd

from math import log2
from toolz import first
from typing import Union
from numbers import Number
from typing import Dict, List

from itertools import groupby
from operator import itemgetter

from cuallee import Check, Rule


class Compute:

    def is_complete(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.not_null().cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_empty(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.is_null().cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def are_complete(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_names = rule.column
        perdicate = [
            daft.col(col_name).not_null().cast(daft.DataType.int64())
            for col_name in col_names
        ]
        col_names_list = [daft.col(col_name) for col_name in rule.column]
        return dataframe.select(*perdicate).sum(col_names_list).to_pandas().astype(
            int
        ).sum().sum() / len(col_names)

    def is_unique(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        perdicate = daft.col(rule.column)
        return (
            dataframe.select(perdicate)
            .distinct()
            .count(perdicate)
            .to_pandas()
            .iloc[0, 0]
        )

    def are_unique(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        # TODO: Find a way to do this in daft and not pandas
        perdicate = [daft.col(col_name) for col_name in rule.column]
        return dataframe.select(*perdicate).to_pandas().nunique().sum() / len(
            rule.column
        )

    def is_greater_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name > rule.value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_greater_or_equal_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name >= rule.value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_less_than(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name < rule.value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_less_or_equal_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name <= rule.value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_equal_than(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name == rule.value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def has_pattern(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (col_name.str.match(rule.value)).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def has_min(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        return (
            dataframe.agg(col_name.min())
            .select(col_name == rule.value)
            .to_pandas()
            .iloc[0, 0]
        )

    def has_max(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        return (
            dataframe.agg(col_name.max())
            .select(col_name == rule.value)
            .to_pandas()
            .iloc[0, 0]
        )

    def has_std(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:

        @daft.udf(return_dtype=daft.DataType.float64())
        def std_dev(data):
            return [statistics.stdev(data.to_pylist())]

        return (
            dataframe.select(std_dev(daft.col(rule.column))).to_pandas().iloc[0, 0]
            == rule.value
        )

    def has_mean(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        return (
            dataframe.agg(col_name.mean())
            .select(col_name == rule.value)
            .to_pandas()
            .iloc[0, 0]
        )

    def has_sum(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        perdicate = daft.col(rule.column)
        return (
            dataframe.select(perdicate).sum(perdicate).to_pandas().iloc[0, 0]
            == rule.value
        )

    def has_cardinality(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        perdicate = daft.col(rule.column)
        return (
            dataframe.select(perdicate)
            .distinct()
            .count(perdicate)
            .to_pandas()
            .iloc[0, 0]
            == rule.value
        )

    def has_infogain(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        perdicate = daft.col(rule.column)
        return (
            dataframe.select(perdicate)
            .distinct()
            .count(perdicate)
            .to_pandas()
            .iloc[0, 0]
            > 1
        )

    def is_between(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (
            (col_name >= min(rule.value)).__and__(col_name <= max(rule.value))
        ).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_contained_in(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        rule_value = list(rule.value) if isinstance(rule.value, tuple) else rule.value
        col_name = daft.col(rule.column)
        perdicate = col_name.is_in(rule_value).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def not_contained_in(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        rule_value = list(rule.value) if isinstance(rule.value, tuple) else rule.value
        col_name = daft.col(rule.column)
        perdicate = col_name.is_in(rule_value).__ne__(True).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def has_percentile(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        # TODO: Find a way to do this in daft and not rely on pandas
        perdicate = daft.col(rule.column)
        return (
            np.percentile(dataframe.select(perdicate).to_pandas().values, rule.settings["percentile"] * 100)  # type: ignore
            == rule.value  # type: ignore
        )

    def has_max_by(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        predicate_1 = daft.col("id")

        max_value = dataframe.agg(daft.col("id2").max()).to_pandas().iloc[0, 0]
        predicate_2 = daft.col("id2").__eq__(max_value)

        return (
            dataframe.where(predicate_2).select(predicate_1).to_pandas().iloc[0, 0]
            == rule.value
        )

    def has_min_by(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        predicate_1 = daft.col("id")

        min_value = dataframe.agg(daft.col("id2").min()).to_pandas().iloc[0, 0]
        predicate_2 = daft.col("id2").__eq__(min_value)

        return (
            dataframe.where(predicate_2).select(predicate_1).to_pandas().iloc[0, 0]
            == rule.value
        )

    def has_correlation(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:

        @daft.udf(return_dtype=daft.DataType.float64())
        def correlation(x, y):
            return [statistics.correlation(x.to_pylist(), y.to_pylist())]

        return (
            dataframe.select(
                correlation(
                    daft.col(rule.column[0])
                    .is_null()
                    .if_else(daft.lit(0), daft.col(rule.column[0]))
                    .alias("x"),
                    daft.col(rule.column[1])
                    .is_null()
                    .if_else(daft.lit(0), daft.col(rule.column[1]))
                    .alias("y"),
                )
            )
            .to_pandas()
            .iloc[0, 0]
            == rule.value
        )

    def satisfies(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        """
        Note: At the moment `daft.DataFrame.where` just accepts Expression not string
        """

        if not isinstance(rule.value, daft.Expression):
            raise ValueError("The value of the rule must be an `daft.Expression`")

        if isinstance(rule.column, list) or isinstance(rule.column, tuple):
            cols = [daft.col(col) for col in rule.column]
            return dataframe.where(rule.value).count(*cols).to_pandas().iloc[0, 0]
        else:
            col = daft.col(rule.column)
            return dataframe.where(rule.value).count(col).to_pandas().iloc[0, 0]

    def has_entropy(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:

        @daft.udf(return_dtype=daft.DataType.float64())
        def entropy(probabilities: daft.Series):
            return [-sum(p * log2(p) for p in probabilities.to_pylist() if p > 0)]

        # Step 1: Cast the column to string and perform groupby and count
        dataframe = (
            dataframe.with_column(
                "label", daft.col(rule.column).cast(daft.DataType.string())
            )
            .groupby("label")
            .count(rule.column)
        )

        # Step 2: Calculate the total sum of counts separately
        total_count = int(
            dataframe.select(daft.col(rule.column)).to_pandas()[rule.column].sum()
        )

        # Step 3: Calculate the probabilities column using the total sum of counts
        dataframe = dataframe.with_column(
            "probabilities", daft.col(rule.column) / total_count
        )

        return dataframe.select(entropy(daft.col("probabilities"))).to_pandas().iloc[
            0, 0
        ] == float(rule.value)

    def is_on_weekday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (
            col_name.dt.day_of_week().is_in([0, 1, 2, 3, 4]).cast(daft.DataType.int64())
        )
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_weekend(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().is_in([5, 6]).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_monday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(0).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_tuesday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(1).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_wednesday(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(2).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_thursday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(3).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_friday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(4).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_saturday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(5).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_sunday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.dt.day_of_week().__eq__(6).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_on_schedule(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = (
            (col_name.dt.hour() >= min(rule.value)).__and__(
                col_name.dt.hour() <= max(rule.value)
            )
        ).cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_daily(self, rule: Rule, dataframe: daft.DataFrame) -> complex:
        if rule.value is None:
            day_mask = [0, 1, 2, 3, 4]
        else:
            day_mask = rule.value

        agg_predicate = [
            daft.col(rule.column).dt.date().min().alias("min"),
            daft.col(rule.column).dt.date().max().alias("max"),
        ]
        select_predicate = [
            daft.col("min").cast(daft.DataType.string()),
            daft.col("max").cast(daft.DataType.string()),
        ]

        lower, upper = (
            dataframe.agg(*agg_predicate)
            .select(*select_predicate)
            .to_pandas()
            .iloc[0]
            .tolist()
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

        filter_predicate = daft.col(rule.column).dt.day_of_week().is_in(day_mask)
        select_predicate = daft.col(rule.column).cast(
            daft.DataType.from_numpy_dtype("datetime64[ms]")
        )
        delivery = (
            dataframe.where(filter_predicate)
            .select(select_predicate)
            .to_pandas()[rule.column]
            .tolist()
        )

        # No difference between sequence of daily as a complex number
        return complex(len(dataframe), len(set(sequence).difference(delivery)))

    def is_inside_interquartile_range(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, complex]:
        col_name = daft.col(rule.column)
        lower, upper = (
            dataframe.select(col_name).to_pandas().quantile(rule.value).values
        )
        perdicate = ((col_name >= lower).__and__(col_name <= upper)).cast(
            daft.DataType.int64()
        )
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def has_workflow(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:

        next_event = "CUALLEE_NEXT_EVENT"
        cuallee_edge = "CUALLEE_EDGE"
        group, event, order = rule.column

        edges = [f"({a}, {b})" for a, b in rule.value]

        @daft.udf(return_dtype=daft.DataType.string())
        def create_edge(x: daft.Series, y: daft.Series):
            return [f"({a}, {b})" for a, b in zip(x.to_pylist(), y.to_pylist())]

        @daft.udf(return_dtype=daft.DataType.string())
        def calculate_next_event(group, order, event):

            # Step 1: Zip the lists together
            zipped_data = list(
                zip(event.to_pylist(), group.to_pylist(), order.to_pylist())
            )

            # Step 2: Sort by group, then by order
            sorted_data = sorted(zipped_data, key=itemgetter(1, 2))

            # Function to calculate the next event within each group
            def calculate_next_event(group):
                items = list(group)
                for i in range(len(items)):
                    # Assign the next event's data if it's not the last item; otherwise, assign None
                    next_event = items[i + 1][0] if i + 1 < len(items) else None
                    yield items[i] + (next_event,)

            # Step 3 & 4: Group by 'group' and calculate the next event
            grouped_data = groupby(sorted_data, key=itemgetter(1))
            result = [
                item
                for _, group in grouped_data
                for item in calculate_next_event(group)
            ]

            _, _, _, next_events = zip(*result)

            return [str(item) for item in next_events]

        return (
            (
                dataframe.with_column(
                    next_event,
                    calculate_next_event(
                        daft.col(group), daft.col(order), daft.col(event)
                    ),
                )
                .with_column(
                    cuallee_edge, create_edge(daft.col(event), daft.col(next_event))
                )
                .select(
                    (daft.col(cuallee_edge).is_in(edges))
                    .alias("key")
                    .cast(daft.DataType.int64())
                )
                .sum(daft.col("key"))
            )
            .to_pandas()
            .iloc[0, 0]
        )


def compute(rules: Dict[str, Rule]):
    """Daft computes directly on the predicates"""
    return True


# TODO: Implement validate_data_types for daft
def validate_data_types(rules: List[Rule], dataframe: daft.DataFrame):
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""
    return True


def summary(check: Check, dataframe: daft.DataFrame):
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
    return daft.from_pylist(computation_basis)
