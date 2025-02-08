from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from cuallee.core.check import ENGINES, Check, CheckLevel, CheckStatus


def test_check_level_values():
    assert CheckLevel.WARNING.value == CheckLevel.WARN.value == 0
    assert CheckLevel.ERROR.value == CheckLevel.ERR.value == 1


def test_check_status_values():
    assert CheckStatus.PASS.value == "PASS"
    assert CheckStatus.FAIL.value == "FAIL"
    assert CheckStatus.NO_RUN.value == "NO_RUN"


class TestCheck:
    @pytest.fixture
    def check(self):
        return Check(
            level=CheckLevel.WARNING,
            name="test_check",
            table_name="test_table",
            config={"key": "value"},
        )

    def test_init_with_int_level(self):
        check = Check(level=0)
        assert check.level == CheckLevel.WARNING

        check = Check(level=1)
        assert check.level == CheckLevel.ERROR

    def test_init_with_check_level(self):
        check = Check(level=CheckLevel.WARNING)
        assert check.level == CheckLevel.WARNING

    def test_init_default_values(self):
        check = Check()
        assert check.level == CheckLevel.WARNING
        assert check.name == "cuallee.check"
        assert isinstance(check.date, datetime)
        assert check.rows == -1
        assert check.config == {}
        assert check.table_name is None
        assert check.dtype == "cuallee.dataframe"
        assert check.session is None

    def test_properties(self, check):
        # Test empty check
        assert check.sum == 0
        assert check.rules == []
        assert check.keys == []
        assert check.empty is True

        # Add a mock rule
        check._rule = {"key1": "rule1", "key2": "rule2"}
        assert check.sum == 2
        assert check.rules == ["rule1", "rule2"]
        assert check.keys == ["key1", "key2"]
        assert check.empty is False

    def test_repr(self, check):
        expected = "Check{'level': 'WARNING', 'name': 'test_check', 'rules': 0, 'table': 'test_table', 'config': {'key': 'value'}}"
        assert repr(check) == expected

    def test_add_rule(self, check):
        # Test adding a rule using method name
        result = check.add_rule("is_unique", "column_name")

        # Verify the rule was added
        assert not check.empty
        assert check.sum == 1

    @pytest.mark.parametrize("engine", ENGINES)
    def test_validate_supported_engines(self, check, engine):
        mock_dataframe = Mock()
        mock_dataframe.__class__.__module__ = f"some_module.{engine}"

        mock_engine = Mock()
        mock_engine.dtypes.return_value = True
        mock_engine.summary.return_value = "summary_result"
        mock_engine.ok.return_value = "ok_result"

        with patch("importlib.import_module", return_value=mock_engine) as mock_import:
            # Add a mock rule to prevent empty check assertion
            check._rule = {"key": "rule"}

            # Test normal validation
            result = check.validate(mock_dataframe)
            assert result == "summary_result"
            mock_import.assert_called_with(f"cuallee.engine.{engine}")

            # Test ok validation
            result = check.validate(mock_dataframe, ok=True)
            assert result == "ok_result"

    def test_validate_empty_check(self, check):
        with pytest.raises(
            AssertionError, match="Check is empty. Try adding some rules?"
        ):
            check.validate(Mock())

    def test_validate_unsupported_engine(self, check):
        mock_dataframe = Mock()
        mock_dataframe.__class__.__name__ = "test.cuallee.dataframe"
        check.is_complete("id")

        with pytest.raises(
            NotImplementedError,
            match="unittest.mock.Mock is not yet implemented in cuallee",
        ):
            check.validate(mock_dataframe)

    def test_validate_invalid_data_types(self, check):
        mock_dataframe = Mock()
        mock_dataframe.__class__.__module__ = "pandas"

        mock_engine = Mock()
        mock_engine.dtypes.return_value = False

        check._rule = {"key": "rule"}  # Add mock rule

        with patch("importlib.import_module", return_value=mock_engine):
            with pytest.raises(
                AssertionError, match="Invalid data types between rules and dataframe"
            ):
                check.validate(mock_dataframe)
