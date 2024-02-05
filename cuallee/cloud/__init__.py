import os
import msgpack
from typing import List, Tuple
import requests
import logging
import json

logger = logging.getLogger("cuallee")


def standardize(check):
    return {
        "n": check.name,
        "d": check.date.strftime("%Y-%m-%d %H:%M:%S"),
        "l": check.level.name,
        "r": check.rows,
        "a": [
            {
                "i": int(r.ordinal),
                "c": str(r.column),
                "m": str(r.method),
                "v": str(r.value),
                "s": int(r.violations),
                "r": float(r.pass_rate),
                "g": float(r.coverage),
                "s": str(r.status),
            }
            for r in check.rules
        ],
    }


def publish(check):
    print(standardize(check))
    requests.post(
        "https://192.168.1.177:5000/metrics",
        data=json.dump(standardize(check)),
        headers={"Content-Type": "application/json"},
        verify=False,
    )
