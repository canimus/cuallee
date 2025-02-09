from ....core.rule import Rule


def is_complete(rule: Rule) -> str:
    """Verify the absence of null values in a column"""
    return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER))"


def is_empty(rule: Rule) -> str:
    """Verify the presence of null values in a column"""
    return f"SUM(CAST({rule.column} IS NULL AS INTEGER))"


def are_complete(rule: Rule) -> str:
    """Verify the abscence of null values on groups of columns"""
    return (
        "SUM( "
        + " + ".join(
            [f"(CAST({column} IS NOT NULL AS INTEGER))" for column in rule.column]
        )
        + f") / {float(len(rule.column))}"
    )


def is_unique(rule: Rule) -> str:
    """Confirms the absence of duplicate values in a column"""
    return f"(COUNT(DISTINCT({rule.column})) == count(*))"


def are_unique(rule: Rule) -> str:
    return (
        "( "
        + " + ".join([f"approx_count_distinct({column})" for column in rule.column])
        + f") / cast({float(len(rule.column))} AS FLOAT)"
    )
