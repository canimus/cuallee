from abc import ABC, abstractmethod

from ..core.rule import Rule


class GenericEngine(ABC):
    """Base class for all generic rule engines"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of BaseEngine as it misses rule container"""
        pass

    @abstractmethod
    def is_complete(self, rule: Rule):
        """Abstract method for is_complete validation"""
        pass

    @abstractmethod
    def is_empty(self, rule: Rule):
        """Abstract method for is_empty validation"""
        pass

    @abstractmethod
    def are_complete(self, rule: Rule):
        """Abstract method for are_complete validation"""
        pass

    @abstractmethod
    def is_unique(self, rule: Rule):
        """Abstract method for is_unique validation"""
        pass

    @abstractmethod
    def are_unique(self, rule: Rule):
        """Abstract method for are_unique validation"""
        pass

    @abstractmethod
    def is_between(self, rule: Rule):
        """Abstract method for is_between validation"""
        pass
