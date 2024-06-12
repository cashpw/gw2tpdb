from typing import Optional, List
from datetime import datetime
from urllib.parse import urlencode
from gw2tpdb.api.endpoint import Endpoint

# ISO-8601: 2020-03-01T13:00:00Z
datawars_date_format = "%Y-%m-%dT%H:%M:%SZ"

def _build_request_url(endpoint: Endpoint, params: dict = {}) -> str:
    """Return encoded request URL with parameters."""

    url = f"{endpoint}"
    if len(params) != 0:
        url += f"?{urlencode(params)}"

    return url

def build_history_request_url(endpoint: Endpoint, item_ids: List[int], start: Optional[datetime] = None, end: Optional[datetime] = None) -> str:
    """Return URL for a /history endpoint request."""

    params = {
        "itemID": ",".join([str(id) for id in item_ids])
    }

    if start:
        params["start"] = start.strftime(datawars_date_format)

    if end:
        params["end"] = end.strftime(datawars_date_format)

    return _build_request_url(endpoint, params)

def build_items_request_url() -> str:
    """Return URL for a /items endpoint request."""

    return _build_request_url(Endpoint.ITEMS_JSON)
