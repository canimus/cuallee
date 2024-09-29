from abc import ABC, abstractmethod
from typing import Any, Dict, List


class ComputeEngine(ABC):
    @abstractmethod
    def __init__(self):
        """Restrict abstract engines and left implementation to each dataframe"""
        pass

    @abstractmethod
    def compute(self, rules: Dict):
        """Returns compute instructions for each rule"""
        pass

    @abstractmethod
    def validate_data_types(self, rules: List, dataframe: Any) -> bool:
        """Validates that all data types from checks match the dataframe with data"""
        pass

    @abstractmethod
    def summary(self, check: Any, dataframe: Any) -> Any:
        """Computes all predicates and expressions for check summary"""
        pass

    @abstractmethod
    def ok(self, check: Any, dataframe: Any) -> Any:
        """Return True or False after validation of a dataframe"""
        pass
