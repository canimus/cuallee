from test.unit.engine.test_base import CualleeTest

import pytest


class TestAreComplete(CualleeTest):

    def logic_positive(self, check, data):
        """Expected to pass with 100% coverage"""
        check.are_complete(("id", "id_desc"))
        rs = check.validate(data)
        assert rs.first().status == "PASS"
        assert rs.first().violations == 0
        assert rs.first().pass_threshold == 1.0
        assert rs.first().pass_rate == 1.0

    def logic_negative(self, check, data):
        """Expected to fail"""
        check.are_complete(("id", "id_desc_with_null"))
        rs = check.validate(data)
        assert rs.first().status == "FAIL"
        assert rs.first().violations == 1
        assert rs.first().pass_threshold == 1.0
        assert rs.first().pass_rate >= 4 / 5

    def get_parameter_config(self):
        """Return parameter config for are_complete tests"""
        return {
            "argvalues": [tuple(["id", "id_desc"]), list(["id", "id_desc"])],
            "ids": ["tuple", "list"],
        }

    def logic_parameters(self, check, data, parameters):
        """Test tuple and list as input for are_complete"""
        check.are_complete(parameters)
        rs = check.validate(data)
        assert rs.first().status == "PASS"

    def logic_coverage(self, check, data):
        """Expected to pass with 70% coverage"""
        check.are_complete(("id", "id_desc_with_null"), 0.7)
        rs = check.validate(data)
        assert rs.first().status == "PASS"
        assert rs.first().violations == 1
        assert rs.first().pass_threshold == 0.7
        assert rs.first().pass_rate >= 4 / 5
