import os
import msgpack
import requests
import logging

logger = logging.getLogger("cuallee")


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


def publish(check):
    """Send results to Cuallee Cloud"""
    try:
        requests.post(
            os.getenv("CUALLEE_CLOUD_HOST"),
            data=msgpack.packb(standardize(check)),
            headers={"Content-Type": "application/octet-stream", "Authorization": f"Bearer {os.getenv('CUALLEE_CLOUD_TOKEN')}"},
            verify=False,
        )
    except (ModuleNotFoundError,KeyError) as error:
        logger.debug(f"Unable to send check to cuallee cloud: {str(error)}")
        pass
