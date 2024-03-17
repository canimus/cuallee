import os
import logging
import requests
from requests.exceptions import ConnectionError

logger = logging.getLogger("cuallee")
CUALLEE_CLOUD_HEADERS = {
    "Content-Type": "application/octet-stream",
    "Authorization": f"Bearer {os.getenv('CUALLEE_CLOUD_TOKEN')}",
}

try:
    import msgpack
except (ModuleNotFoundError, ImportError):
    logger.error("Module msgpack missing for cloud operations")


def standardize(check):
    return {
        "name": check.name,
        "date": check.date.strftime("%Y-%m-%d %H:%M:%S"),
        "level": check.level.name,
        "rows": check.rows,
        "rules": [
            {
                "ord": int(r.ordinal),
                "col": str(r.column),
                "met": str(r.method),
                "val": str(r.value),
                "vio": int(r.violations),
                "psr": float(r.pass_rate),
                "cov": float(r.coverage),
                "sta": str(r.status),
            }
            for r in check.rules
        ],
    }


def compress(check):
    return msgpack.packb(standardize(check))


def publish(check):
    """Send results to Cuallee Cloud"""
    try:
        requests.post(
            os.getenv("CUALLEE_CLOUD_HOST"),
            data=compress(check),
            headers=CUALLEE_CLOUD_HEADERS,
            verify=False,
        )
    except (ModuleNotFoundError, KeyError, ConnectionError) as error:
        logger.debug(f"Unable to send check to cuallee cloud: {str(error)}")
        pass
