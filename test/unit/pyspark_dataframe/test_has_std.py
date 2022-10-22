from cuallee import Check, CheckLevel
import numpy as np
import logging
import pytest

logger = logging.getLogger(__name__)

@pytest.mark.statistical
def test_sigmas(spark):
    df = spark.range(10)
    check = Check(CheckLevel.ERROR, "StdDevTest")
    check.has_std("id", np.arange(10).std())
    results = check.validate(df)
    logger.debug(results.collect())
    assert results.first().status == "PASS"
