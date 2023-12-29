from cuallee import Control
import pandas as pd

def test_has_completeness():
    assert hasattr(Control, "completeness")

def test_completeness_result():
    df = pd.DataFrame({"A" : [1,2,3,4,5]})
    assert Control.completeness(df).status.eq("PASS").all()