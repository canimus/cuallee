from .. import Check
import pandas as pd
from pathlib import Path


class BioChecks:
    def __init__(self, check: Check):
        self._check = check
        try:
            parent_path = Path(__file__).parent
            self._aminoacids = pd.read_csv(parent_path / "amino_acids.csv")
        except Exception:
            raise Exception("Unable to load aminoacid definitions")

    def is_dna(self, column: str):
        """Validates that a sequence contains only valid nucleotide bases of DNA strand"""
        self._check.has_pattern(column, r"^[GTCA]*$")
        return self._check

    def is_protein(self, column: str):
        """Verifies that country codes are valid against the ISO standard 3166"""
        self._check.has_pattern(
            column, rf"^[{''.join(self._aminoacids['1_letter_code'].tolist())}]*$"
        )
        return self._check
