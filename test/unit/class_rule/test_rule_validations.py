import pytest
from cuallee import Rule, CheckDataType


def test_negative_coverage():
    with pytest.raises(ValueError, match="Coverage should be between 0 and 1"):
        Rule("is_unique", "id", None, CheckDataType.NUMERIC, -1)


def test_representation():
    rule = Rule("is_unique", "id", None, CheckDataType.NUMERIC)
    out = str(repr(rule))
    assert "Rule(method:is_unique" in out
