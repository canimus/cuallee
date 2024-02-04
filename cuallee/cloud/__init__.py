import os
import msgpack
from typing import List, Tuple
import asyncio


def publish(result: List[Tuple]) -> bool:
    """Sends test results to Cuallee Cloud"""
    print("Sending data to cuallee.cloud")
    
