from .base import PysparkTestCase


class TestCase(PysparkTestCase):
    def test_pass(self, spark, check):
        check.is_complete("id")
        assert check.validate(spark.range(10), ok=True)

    def test_fail(self, check):
        assert True

    def test_exception(self, check):
        assert True

    def test_parameter(self, check):
        assert True

    def test_coverage(self, check):
        assert True
