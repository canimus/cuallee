from lxml import etree
import requests
from dataclasses import dataclass
from typing import List
import os
from operator import attrgetter as at
from functools import lru_cache

@dataclass
class Currency:
    country_name: str
    currency_name: str
    currency: str = None
    currency_number: str = None
    currency_units: str = None

@lru_cache
def _load_currencies(by_field : str):
    """Internal helper method to load all currencies from default environment location"""
    print("Downloading...")
    DEFAULT_ENDPOINT = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"
    response = requests.get(os.getenv("ISO_4217_ENDPOINT", DEFAULT_ENDPOINT))
    xml = etree.fromstring(response.text.encode("utf-8"))

    def _get_ccy(element : etree._Element) -> Currency:
        return at(by_field)(Currency(*[tag.text for tag in element.getchildren()]))

    return set(filter(None, (map(_get_ccy, xml.xpath("//CcyNtry")))))

class ISO():
    CCY_CODE = "currency"
    CCY_NUMBER = "currency_number"

    def __init__(self, check, currency_field : str = CCY_CODE):
        self._check = check
        self._field = currency_field
        self._ccy = []

    def iso_4217(self, column : str):
        """It verifies a field against the international standard currency codes via code or number fields from ISO 4217"""
        self._ccy = _load_currencies(self._field)
        self._check.is_in(column, self._ccy)
    