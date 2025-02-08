import enum
import importlib
import operator
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple, Union

from pyspark.sql import Column, DataFrame
from toolz import first, valfilter, valmap


class ComputeMethod(enum.Enum):
    OBSERVE = "OBSERVE"
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, List[Column], None]
    expression: Union[Callable[[DataFrame, str], Any], Column]
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


def has_observe(spark) -> bool:
    major, minor, _ = map(int, spark.version.split("."))
    with_observe = False
    if not all(map(lambda x: x > 3, [major, minor])) or ("connect" in str(type(spark))):
        with_observe = True

    return with_observe


def make_backwards_compatible(computed_expressions: dict) -> dict:
    """As Observe API is not available replace instructions to select statements"""
    return valmap(
        lambda v: ComputeMethod.SELECT if v == ComputeMethod.OBSERVE else v,
        computed_expressions,
    )


def compute_observations(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Tuple[int, Dict]:
    """Compute rules throught spark Observation"""

    # Filter expression directed to observe
    _observe = lambda x: x.compute_method.name == ComputeMethod.OBSERVE.name
    observe = valfilter(_observe, compute_set)

    if observe:
        from pyspark.sql import Observation

        observation = Observation("observation")

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in observe.items()
            ],
        )
        rows = df_observation.count()
        return rows, observation.get
    else:
        # observation_result = {}
        rows = dataframe.count()
        return rows, {}


def compute_selections(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark select"""

    # Filter expression directed to select
    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in select.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )


def compute_transformations(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark transform"""

    # Filter expression directed to transform
    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())  # type: ignore
        for k, compute_instruction in transform.items()
    }


def find_spark(config: Dict):
    """Determine if is spark or spark_connect"""
    from pyspark.sql import SparkSession

    # Search for client using spark_connect
    if "SPARK_REMOTE" in os.environ:
        try:
            spark_connect = importlib.import_module("pyspark.sql.connect.session")
            spark = (
                getattr(spark_connect, "SparkSession")
                .builder.remote(os.getenv("SPARK_REMOTE"))
                .getOrCreate()
            )
        except (ModuleNotFoundError, ImportError, AttributeError):
            pass

    elif spark_in_session := valfilter(
        lambda x: isinstance(x, SparkSession), globals()
    ):
        # Obtain the first spark session available in the globals
        spark = first(spark_in_session.values())
    else:
        builder = SparkSession.builder
        # Retrieve config settings from check
        if config and isinstance(config, dict) and len(config.keys()):
            for k, v in config.items():
                builder.config(k, v)
        spark = builder.getOrCreate()

    return spark
