import logging

from typing import Any, Optional, List
from datetime import datetime, timezone, timedelta
from gw2tpdb.db import db
from gw2tpdb.api.datawars import get_daily, get_items
from gw2tpdb.api.history import HistoryEntry, row_to_history_entry, history_entry_to_tuple
from gw2tpdb.api.item import ItemEntry, row_to_item_entry, item_entry_to_tuple

logger = logging.getLogger(__name__)


class Gw2TpDb():
    """TODO"""

    def __init__(self, database_path, auto_update: bool = False):
        """TODO

        - auto_update: `update_` automatically before any `get_`"""

        self._auto_update = auto_update
        self.conn = db.connect(database_path)

    def __del__(self):
        """TODO"""
        self.conn.close()

    def update_daily(self, item_id: int) -> bool:
        """Download and write missing daily data for item_id to database.

        Return false when unsuccessful.

        Idempotent."""

        logger.debug(f"Attempting to update item_id {item_id}")

        most_recent_datetime = self._most_recent_daily_history_datetime(item_id)
        if most_recent_datetime is None:
            logger.debug(f"Daily history data for item_id {item_id} not found in database. Will download full history.")
            daily_data_opt = get_daily(item_id)
        else:
            most_recent_date = most_recent_datetime.date()
            yesterday_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

            if most_recent_date >= yesterday_date:
                logger.debug(f"Daily history data for item_id {item_id} is up to date. Skipping update.")
                return True
            else:
                logger.debug(f"Daily history data for item_id {item_id} is out of date. Most recent available data is dated at {yesterday_date} whereas the latest in database is {most_recent_date}. Will download partial history.")
                daily_data_opt = get_daily(item_id, start=most_recent_date)

        if daily_data_opt is None:
            logger.error(f"Daily history data download returned None. Cannot update item_id {item_id} in database.")
            return False
        daily_data = daily_data_opt

        self._write_daily(daily_data)

        return True

    def populate_items(self) -> bool:
        """Download and write item data to database.

        Idempotent."""
        logger.debug(f"Attempting to populate items table")

        if self._items_table_populated():
            logger.debug(f"Item table already populated")
            return False

        items_opt = get_items()
        if items_opt is None:
            logger.debug(f"ItemEntry data missing. Cannot update items table.")
            return False
        items = items_opt

        self._write_items(items)

        return True

    def get_daily(self, item_id: int) -> Optional[List[HistoryEntry]]:
        """Return daily data for ITEM_ID."""
        if self._auto_update:
            self.update_daily(item_id)

        rows_opt = self._execute(f"SELECT * FROM daily_history WHERE id = {item_id} ORDER BY utc_timestamp ASC")
        if rows_opt is None:
            logger.debug(f"No items (id = {item_id}) found in database")
            return None
        rows = rows_opt

        return list(map(row_to_history_entry, rows))

    def get_dailies(self, item_ids: List[int]) -> Optional[dict[List[HistoryEntry]]]:
        """Return daily data for ITEM_ID."""
        if self._auto_update:
            for item_id in item_ids:
                self.update_daily(item_id)

        rows_opt = self._execute(f"SELECT * FROM daily_history WHERE id IN ({','.join(list(map(str, item_ids)))}) ORDER BY utc_timestamp ASC")
        if rows_opt is None:
            logger.debug(f"Database returned no rows")
            return None
        rows = rows_opt

        dailies = {}
        for entry in list(map(row_to_history_entry, rows)):
            if not entry.id in dailies:
                dailies[entry.id] = []
            dailies[entry.id] += [entry]

        return dailies

    def _daily_history_ids(self, ):
        """Return list of unique IDs in daily_history table."""

        result = self.conn.cursor().execute(f"SELECT DISTINCT id FROM daily_history")


    def _write_items(self, entries: List[ItemEntry]) -> None:
        """Write given items to database."""

        # TODO: Replace "items" with variable
        self._insert_many("items", list(map(item_entry_to_tuple, entries)))

    def _write_daily(self, entries: List[HistoryEntry]) -> None:
        """Write given daily history data to database."""

        # TODO: Replace "daily_history" with variable
        self._insert_many("daily_history", list(map(history_entry_to_tuple, entries)))

    def _items_table_populated(self) -> bool:
        """Return true if the items table has any rows."""

        result = self._execute("SELECT COUNT(*) FROM items LIMIT 1")
        if result is None or result[0][0] is None:
            logger.debug(f"Items table is empty")
            return False

        return True

    def _most_recent_daily_history_datetime(self, item_id: int) -> Optional[datetime]:
        """Return most recent timestamp for item_id in daily table."""

        # TODO: Replace "daily_history" with variable
        result = self._execute(f"SELECT MAX(utc_timestamp) FROM daily_history WHERE id = {item_id}")
        if result is None or result[0][0] is None:
            logger.debug(f"No daily history data found for item_id {item_id}")
            return None
        most_recent_timestamp = result[0][0]
        most_recent_datetime = datetime.fromtimestamp(most_recent_timestamp, tz=timezone.utc)

        logger.debug(f"Most recent daily history data for item_id {item_id} is from {most_recent_datetime.isoformat()} ({most_recent_timestamp})")

        return most_recent_datetime

    def _execute(self, query: str) -> Optional[List[tuple]]:
        """Execute given query and return results if present."""

        result = self.conn.cursor().execute(query)
        if result is None:
            return None

        return result.fetchall()

    def _insert_many(self, table_name: str, rows: List[tuple]) -> None:
        """Insert rows into table."""

        if len(rows) == 0:
            logger.debug(f"Cannot insert an empty list into {table_name}")
            return None

        if len(rows[0]) == 0:
            logger.debug(f"Cannot insert empty tuples into {table_name}")
            return None

        field_count = len(rows[0])
        question_marks = ",".join(list("?" * field_count))
        logger.debug(f"Inserting {len(rows)} rows into {table_name}")
        self.conn.cursor().executemany(f"INSERT INTO {table_name} VALUES({question_marks})", rows)
        self.conn.commit()
        logger.debug(f"Inserted {len(rows)} rows into {table_name}")
