import duckdb

from cuallee.core.check import Check

from ..base import DuckDbTestCase  # type: ignore


class TestCase(DuckDbTestCase):
    def test_pass(self, check: Check, db: duckdb.DuckDBPyConnection):
        check.are_unique(("ID", "ID2"))
        assert self.evaluate(check.validate(db))

    def test_fail(self, db: duckdb.DuckDBPyConnection):
        check = Check(table_name="DUPS")
        check.are_unique(("ID4", "ID"))
        assert self.evaluate(check.validate(db), passed=False)

    def test_exception(self):
        assert True

    def test_parameter(self):
        assert True

    def test_coverage(self):
        assert True
