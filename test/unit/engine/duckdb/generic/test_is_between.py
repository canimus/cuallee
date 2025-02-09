import duckdb

from cuallee.core.check import Check

from ..base import DuckDbTestCase  # type: ignore


class TestCase(DuckDbTestCase):
    def test_pass(self, check: Check, db: duckdb.DuckDBPyConnection):
        check.are_complete(("ID", "ID2"))
        check.validate(db)
        assert True

    def test_fail(self):
        assert True

    def test_exception(self):
        assert True

    def test_parameter(self):
        assert True

    def test_coverage(self):
        assert True
