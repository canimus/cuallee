import operator
from typing import List

from toolz import compose
from toolz.curried import map as map_curried

from .check import Check, CheckLevel


class Control:
    @staticmethod
    def completeness(dataframe, name: str = "cuallee.completeness", **kwargs):
        """Control of null values on data frames"""
        check = Check(CheckLevel.WARNING, name=name, **kwargs)
        for column in dataframe.columns:
            check.is_complete(column)
        return check.validate(dataframe)

    @staticmethod
    def information(dataframe, **kwargs):
        """Information gain"""
        check = Check(CheckLevel.WARNING, name="cuallee.information", **kwargs)
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
        result = list(
            compute(
                Control.completeness(
                    dataframe, name="cuallee.percentage_fill", **kwargs
                )
            )
        )
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
