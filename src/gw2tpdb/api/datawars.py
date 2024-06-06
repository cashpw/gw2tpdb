import time
import logging
import requests
import json

from ratelimit import limits, sleep_and_retry
from datetime import datetime, timedelta
from typing import Optional, List, TypeVar, Callable
from gw2tpdb.api.item import ItemEntry, item_json_to_dataclass
from gw2tpdb.api.history import HistoryEntry, history_json_to_dataclass
from gw2tpdb.api.url import build_history_request_url, build_items_request_url
from gw2tpdb.api.endpoint import Endpoint

T = TypeVar("T")

logger = logging.getLogger(__name__)

deadline_seconds = 5

# Limit to 1 QPS to be kind to the non-profit API host.
@sleep_and_retry
@limits(calls=1, period=timedelta(seconds=1).total_seconds())
def _datawars_get(url: str) -> Optional[requests.Response]:
    """Make a request to the Datawars API and return the resulting JSON if successful."""

    try:
        response = requests.get(url, timeout=deadline_seconds)
        response.raise_for_status()

        return response
    except requests.exceptions.Timeout as e:
        logger.error(f"Timed out (deadline={deadline_seconds} seconds) getting '{url}': {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting '{url}': {e}")

    return None

def _datawars_get_as_json(url: str) -> Optional[dict]:
    """Make a request to the Datawars API and parse response as json."""

    try:
        return _datawars_get(url).json()
    except Exception as e:
        logger.error(f"Failed to parse JSON response for {url} : {e}")
        return None

def _datawars_get_as_dataclass_list(url: str, json_to_dataclass: Callable[dict, T]) -> Optional[List[T]]:
    """Make a request to the Datawars API and parse json response into dataclass."""

    json_opt = _datawars_get_as_json(url)
    if json_opt is None:
        return None
    json = json_opt

    return list(map(json_to_dataclass, json))

def get_items() -> Optional[List[ItemEntry]]:
    """Fetch items."""

    return _datawars_get_as_dataclass_list(build_items_request_url(), item_json_to_dataclass)

def get_daily(item_id: int, start: Optional[datetime] = None, end: Optional[datetime] = None) -> Optional[List[HistoryEntry]]:
    """Fetch daily historic data for given ITEM_ID."""

    return _datawars_get_as_dataclass_list(build_history_request_url(Endpoint.HISTORY_DAILY_JSON, item_id, start, end), history_json_to_dataclass)


def get_hourly(item_id: int, start: Optional[datetime] = None, end: Optional[datetime] = None) -> Optional[List[HistoryEntry]]:
    """Fetch hourly historic data for given ITEM_ID."""

    return _datawars_get_as_json(build_history_request_url(Endpoint.HISTORY_HOURLY_JSON, item_id, start, end), history_json_to_dataclass)
