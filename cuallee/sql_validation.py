from typing import Dict, Union, List
from cuallee import Check, Rule
import duckdb as dk
import operator
import numpy as np
import re
from toolz import first  # type: ignore
from numbers import Number
from cuallee import utils as cuallee_utils
from pyspark.sql.session import SparkSession


class Compute:
    def is_complete(
        self, rule: Rule, connection: Union[dk.DuckDBPyConnection, SparkSession]
    ) -> Union[bool, int]:
        return connection.execute(
            f"SELECT sum(cast({rule.column} IS NULL AS INTEGER)) from {rule.table}"
        )
