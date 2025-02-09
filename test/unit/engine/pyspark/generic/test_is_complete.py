from ..base import PysparkTestCase


class TestCase(PysparkTestCase):
    def test_pass(self, spark, check):
        check.is_complete("id_1")
        assert check.validate(self.get_data(spark), ok=True)

    def test_fail(self, spark, check):
        check.is_complete("id_3")
        assert not check.validate(self.get_data(spark), ok=True)

    def test_exception(self, check):
        assert True

    def test_parameter(self, check):
        assert True

    def test_coverage(self, check):
        assert True
