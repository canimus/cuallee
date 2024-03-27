import daft

from cuallee import Check, CheckLevel

df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
check = Check(CheckLevel.WARNING, "CompletePredicate")
check.are_unique(("id", "id2"), 0.5)

# Validate
print(check.validate(df))