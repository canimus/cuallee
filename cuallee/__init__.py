from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark import spark as sp
from pyspark.sql import Window as W


@dataclass
class Validation:
    name: str
    date: datetime
    description: str
    status: str

def is_complete(df : Union[sp.DataFrame, pd.DataFrame]) -> List[Validation]:
    pass