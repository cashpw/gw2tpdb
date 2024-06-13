import logging

from datetime import datetime, timezone
from dataclasses import dataclass

# TODO: Rename utc_timestamp to utc_datetime

@dataclass
class HistoryEntry():
    """Represents a single entry of history data."""

    id: int
    buy_delisted: int
    buy_listed: int
    buy_price_avg: int
    buy_price_max: int
    buy_price_min: int
    buy_price_stdev: float
    buy_quantity_avg: int
    buy_quantity_max: int
    buy_quantity_min: int
    buy_quantity_stdev: float
    buy_sold: int
    buy_value: int
    count: int
    sell_delisted: int
    sell_listed: int
    sell_price_avg: int
    sell_price_max: int
    sell_price_min: int
    sell_price_stdev: float
    sell_quantity_avg: int
    sell_quantity_max: int
    sell_quantity_min: int
    sell_quantity_stdev: float
    sell_sold: int
    sell_value: int
    utc_timestamp: int

def history_json_to_dataclass(json: dict) -> HistoryEntry:
    """Return HisoryEntry object populated from JSON."""

    return HistoryEntry(
        id=json["itemID"],
        buy_delisted=json["buy_delisted"] if "buy_delisted" in json else 0,
        buy_listed=json["buy_listed"] if "buy_listed" in json else 0,
        buy_price_avg=json["buy_price_avg"] if "buy_price_avg" in json else 0,
        buy_price_max=json["buy_price_max"] if "buy_price_max" in json else 0,
        buy_price_min=json["buy_price_min"] if "buy_price_min" in json else 0,
        buy_price_stdev=json["buy_price_stdev"] if "buy_price_stdev" in json else 0,
        buy_quantity_avg=json["buy_quantity_avg"] if "buy_quantity_avg" in json else 0,
        buy_quantity_max=json["buy_quantity_max"] if "buy_quantity_max" in json else 0,
        buy_quantity_min=json["buy_quantity_min"] if "buy_quantity_min" in json else 0,
        buy_quantity_stdev=json["buy_quantity_stdev"] if "buy_quantity_stdev" in json else 0,
        buy_sold=json["buy_sold"] if "buy_sold" in json else 0,
        buy_value=json["buy_value"] if "buy_value" in json else 0,
        count=json["count"] if "count" in json else 0,
        sell_delisted=json["sell_delisted"] if "sell_delisted" in json else 0,
        sell_listed=json["sell_listed"] if "sell_listed" in json else 0,
        sell_price_avg=json["sell_price_avg"] if "sell_price_avg" in json else 0,
        sell_price_max=json["sell_price_max"] if "sell_price_max" in json else 0,
        sell_price_min=json["sell_price_min"] if "sell_price_min" in json else 0,
        sell_price_stdev=json["sell_price_stdev"] if "sell_price_stdev" in json else 0,
        sell_quantity_avg=json["sell_quantity_avg"] if "sell_quantity_avg" in json else 0,
        sell_quantity_max=json["sell_quantity_max"] if "sell_quantity_max" in json else 0,
        sell_quantity_min=json["sell_quantity_min"] if "sell_quantity_min" in json else 0,
        sell_quantity_stdev=json["sell_quantity_stdev"] if "sell_quantity_stdev" in json else 0,
        sell_sold=json["sell_sold"] if "sell_sold" in json else 0,
        sell_value=json["sell_value"] if "sell_value" in json else 0,
        utc_timestamp=datetime.fromisoformat(json["date"]))

def row_to_history_entry(row: tuple) -> HistoryEntry:
    """Build and return a HistoryEntry from ROW."""

    return HistoryEntry(
        id=row[0],
        buy_delisted=row[1],
        buy_listed=row[2],
        buy_price_avg=row[3],
        buy_price_max=row[4],
        buy_price_min=row[5],
        buy_price_stdev=row[6],
        buy_quantity_avg=row[7],
        buy_quantity_max=row[8],
        buy_quantity_min=row[9],
        buy_quantity_stdev=row[10],
        buy_sold=row[11],
        buy_value=row[12],
        count=row[13],
        sell_delisted=row[14],
        sell_listed=row[15],
        sell_price_avg=row[16],
        sell_price_max=row[17],
        sell_price_min=row[18],
        sell_price_stdev=row[19],
        sell_quantity_avg=row[20],
        sell_quantity_max=row[21],
        sell_quantity_min=row[22],
        sell_quantity_stdev=row[23],
        sell_sold=row[24],
        sell_value=row[25],
        utc_timestamp=datetime.fromtimestamp(row[26], tz=timezone.utc))

def history_entry_to_tuple(history_entry: HistoryEntry) -> tuple:
    """Return given HistoryEntry as a tuple."""

    return (history_entry.id,
        history_entry.buy_delisted,
        history_entry.buy_listed,
        history_entry.buy_price_avg,
        history_entry.buy_price_max,
        history_entry.buy_price_min,
        history_entry.buy_price_stdev,
        history_entry.buy_quantity_avg,
        history_entry.buy_quantity_max,
        history_entry.buy_quantity_min,
        history_entry.buy_quantity_stdev,
        history_entry.buy_sold,
        history_entry.buy_value,
        history_entry.count,
        history_entry.sell_delisted,
        history_entry.sell_listed,
        history_entry.sell_price_avg,
        history_entry.sell_price_max,
        history_entry.sell_price_min,
        history_entry.sell_price_stdev,
        history_entry.sell_quantity_avg,
        history_entry.sell_quantity_max,
        history_entry.sell_quantity_min,
        history_entry.sell_quantity_stdev,
        history_entry.sell_sold,
        history_entry.sell_value,
        history_entry.utc_timestamp.timestamp())
