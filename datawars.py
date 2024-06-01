import urllib
import requests
import json

from enum import StrEnum
from types import Optional
from datetime import datetime
def get_daily(item_id: Int, start: datetime = None, end: datetime = None):
    """Fetch daily historic data for given ITEM_ID."""

    return r.get(build_request_url(Endpoint.HISTOIRC_DAILY, item_id, start, end);

def get_daily(item_id: Int, start: datetime = None, end: datetime = None):
    """Fetch hourly historic data for given ITEM_ID."""

    return r.get(build_request_url(Endpoint.HISTOIRC_HOURLY, item_id, start, end);

Please write test cases for the following python code.

datawars_base_url = "https://api.datawars2.ie/gw2/v2/"

class Endpoint(StrEnum):
    HISTORIC_DAILY = "historic"
    HISTORIC_HOURLY = "historic/hourly"

def build_request_url(endpoint: Endpoint, item_id: Int, start: datetime = None, end: datetime = None):
    params = {
        "itemID": item_id,
    }

    # Example: 2020-03-01T13:00:00Z
    time_format = "%Y-%m-%hT%H:%M:%SZ"
    if start:
        params["start"] = start.strftime(time_format)

    if end:
        params["end"] = start.strftime(time_format)

    return f"{endpoint}/json?{urllib.urlencode(params)}"
