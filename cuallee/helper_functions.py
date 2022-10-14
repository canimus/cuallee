from typing import Dict


def _delete_dict_entry(keys: str, dictionary_1: Dict, dictionary_2: Dict):
    """Delete entrie(s) from to related dictionary based on their keys."""
    for key in keys:
        dictionary_1.pop(key)
        if key in dictionary_2.keys():
            dictionary_2.pop(key)
