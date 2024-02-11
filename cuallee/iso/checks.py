import xml.etree.ElementTree as ET
import requests
from dataclasses import dataclass
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
def _load_currencies():
    """External download from ISO website of currencies in XML format"""
    DEFAULT_ENDPOINT_4217 = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"
    response = requests.get(os.getenv("ISO_4217_ENDPOINT", DEFAULT_ENDPOINT_4217))
    xml = ET.fromstring(response.text.encode("utf-8"))

    def _get_ccy(element):
        currency_data = {tag.tag: tag.text.strip() for tag in list(element)}
        return at("currency")(Currency(*currency_data.values()))

    return set(filter(None, (map(_get_ccy, xml.findall("./CcyTbl/CcyNtry")))))


@lru_cache
def _load_countries():
    """External download from Google shared data of country codes and locations"""
    DEFAULT_ENDPOINT_3166 = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.xml"
    response = requests.get(os.getenv("ISO_4217_ENDPOINT", DEFAULT_ENDPOINT_3166))
    xml = ET.fromstring(response.text.encode("utf-8"))
    return list(map(lambda x: x.attrib["alpha-2"], xml.findall("./country")))


class ISO:
    """Abstraction of checks related to ISO standards"""

    CCY_CODE = "currency"
    CCY_NUMBER = "currency_number"
    COUNTRY_CODE = "country"
    COUNTRY_NAME = "name"

    def __init__(self, check):
        self._check = check
        self._ccy = []
        self._countries = []

    def iso_4217(self, column: str):
        """It verifies a field against the international standard currency codes via code or number fields from ISO 4217"""
        self._ccy = _load_currencies()
        self._check.is_in(column, self._ccy)
        return self._check

    def iso_3166(self, column: str):
        """Verifies that country codes are valid against the ISO standard 3166"""
        self._countries = _load_countries()
        self._check.is_in(column, self._countries)
        return self._check

    iso_currencies = iso_4217
    iso_countries = iso_3166
