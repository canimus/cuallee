import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from typing import List

def numeric_fields(dataframe : DataFrame) -> List[str]:
        """ Filter all numeric data types in data frame and returns field names """
        return set([f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.NumericType)])
