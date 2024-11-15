from cuallee.core.check import Check, CheckLevel


def test_representation():
    check = Check(CheckLevel.WARNING, "Repr")
    out = repr(check)
    assert "Check(level:0"


def test_numeric_level():
    check = Check(1, "Repr")
    out = repr(check)
    assert "Check(level:CheckLevel.ERROR" in out


def test_with_table():
    check = Check(0, "Repr", table_name="users")
    out = repr(check)
    assert ("Check(level:CheckLevel.WARNING" in out) & ("table:users" in out)


def test_sum():
    check = Check(0, "Sum")
    check.is_complete("id")
    check.is_unique("id")
    assert check.sum == 2


def test_keys():
    check1 = Check(0, "Sum")
    check1.is_complete("id")
    check1.is_unique("id")
    check2 = Check(0, "Sum")
    check2.is_complete("id")
    check2.is_unique("id")
    assert set(check1.keys) == set(check2.keys)


def test_adjust_coverage():
    check = Check(0, "Sum")
    check.is_complete("id", pct=0.8)
    check.adjust_rule_coverage(0, 0.9)
    assert check.rules[0].coverage == 0.9
