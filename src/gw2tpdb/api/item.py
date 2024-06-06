from enum import Enum
from dataclasses import dataclass

@dataclass
class ItemEntry():
    """Represents a single item."""

    id: int
    name: str

def item_json_to_dataclass(json: dict) -> ItemEntry:
    """Return ItemEntry object populated from JSON."""

    return ItemEntry(
        id=json["id"],
        name=json["name"])

def row_to_item_entry(row: tuple) -> ItemEntry:
    """Build and return a ItemEntry from ROW."""

    return ItemEntry(
        id=row[0],
        name=row[1])

def item_entry_to_tuple(item_entry: ItemEntry) -> tuple:
    """Return given ItemEntry as a tuple."""

    return (item_entry.id,
       item_entry.name)
