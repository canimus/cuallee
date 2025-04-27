from test.unit.engine.test_base import CualleeTest


class TestIsEmpty(CualleeTest):

    def logic_positive(self, check, data):
        """Expected to pass with 100% coverage"""
        check.is_empty("id_empty")
        rs = check.validate(data)
        assert rs.first().status == "PASS"
        assert rs.first().violations == 0
        assert rs.first().pass_threshold == 1.0
        assert rs.first().pass_rate == 1.0

    def logic_negative(self, check, data):
        """Expected to fail"""
        check.is_empty("id_with_null")
        rs = check.validate(data)
        assert rs.first().status == "FAIL"
        assert rs.first().violations == 4
        assert rs.first().pass_threshold == 1.0
        assert rs.first().pass_rate <= 2 / 5

    def get_parameter_config(self):
        """Configure parameters for test_parameters - skip if no parameters needed"""
        return "😅 No parameters to be tested!"

    def logic_parameters(self, check, data, parameters):
        return "😅 No parameters to be tested!"

    def logic_coverage(self, check, data):
        """Expected to pass with 70% coverage"""
        check.is_empty("id_with_null", 0.1)
        rs = check.validate(data)
        assert rs.first().status == "PASS"
        assert rs.first().pass_threshold == 0.1
        assert rs.first().pass_rate <= 2 / 5
