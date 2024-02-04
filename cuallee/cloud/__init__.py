import os
import msgpack
from typing import List, Tuple
import asyncio


def publish(check) -> bool:
    """Sends test results to Cuallee Cloud"""
    print("Sending data to cuallee.cloud")
