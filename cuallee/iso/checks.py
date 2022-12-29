from lxml import etree
import requests
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List
import os
from operator import attrgetter as at

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

    def __init__(self, check):
        self._check = check

    def iso_4217(self, column : str, by_field : str = CCY_CODE):
        """It verifies a field against the international standard currency codes via code or number fields from ISO 4217"""
        load_dotenv()
        response = requests.get(os.getenv("ISO_4217_ENDPOINT"))
        xml = etree.fromstring(response.text.encode("utf-8"))

        def _get_ccy(element : etree._Element) -> Currency:
            return at(by_field)(Currency(*[tag.text for tag in element.getchildren()]))

        codes = set(filter(None, (map(_get_ccy, xml.xpath("//CcyNtry")))))
        self._check.is_in(column, codes)
    