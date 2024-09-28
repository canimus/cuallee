from .. import Check
from typing import Dict
from toolz import compose

class LogicCheck:
    """Performs logical inference on evaluated checks"""

    def __init__(self, checks: Dict[str, Check] = {}):
        pass
