from lxml import etree
import requests
from dotenv import load_dotenv
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

class ISO():
    CCY_CODE = "currency"
    CCY_NUMBER = "currency_number"

    @lru_cache
    def _load_currencies(self, by_field : str = CCY_CODE):
        """Internal helper method to load all currencies from default environment location"""
        load_dotenv()
        response = requests.get(os.getenv("ISO_4217_ENDPOINT"))
        xml = etree.fromstring(response.text.encode("utf-8"))

        def _get_ccy(element : etree._Element) -> Currency:
            return at(by_field)(Currency(*[tag.text for tag in element.getchildren()]))

        return set(filter(None, (map(_get_ccy, xml.xpath("//CcyNtry")))))

    def __init__(self, check, currency_field : str = CCY_CODE):
        self._check = check
        self._ccy = self._load_currencies(currency_field)

    def iso_4217(self, column : str):
        """It verifies a field against the international standard currency codes via code or number fields from ISO 4217"""
        self._check.is_in(column, self._ccy)
    