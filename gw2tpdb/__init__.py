import logging
import math
import pytz

from typing import Any, Optional, List
from datetime import datetime, timezone, timedelta
from gw2tpdb.db import db
from gw2tpdb.api.datawars import get_daily, get_items, get_dailies
from gw2tpdb.api.history import HistoryEntry, row_to_history_entry, history_entry_to_tuple
from gw2tpdb.api.item import ItemEntry, row_to_item_entry, item_entry_to_tuple

logger = logging.getLogger(__name__)
glob_of_ectoplasm_id = 19721

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

    def update_daily(self, item_id: int, full_download: bool = False) -> bool:
        """Download and write missing daily data for item_id to database.

        Return false when unsuccessful.

        Skip checks if start provided.

        Idempotent."""

        if full_download:
            logger.debug(f"Daily history data for item_id {item_id} not found in database. Will download full history.")
            start = None
        else:
            most_recent_local_timestamp_opt = self._most_recent_local_daily_timestamp(item_id)
            if most_recent_local_timestamp_opt is None:
                logger.debug(f"Daily history data for item_id {item_id} not found in database. Will download full history.")
                start = None
            else:
                most_recent_local_timestamp = most_recent_local_timestamp_opt
                most_recent_remote_timestamp = self._most_recent_remote_daily_timestamp()
                logger.debug(f"Most recent remote data is dated {most_recent_remote_timestamp}")
                if most_recent_local_timestamp > most_recent_remote_timestamp:
                    logger.debug(f"Daily history data for item_id {item_id} is more recent than most-recent remote data. Skipping update.")
                    return True
                if most_recent_local_timestamp == most_recent_remote_timestamp:
                    logger.debug(f"Daily history data for item_id {item_id} is up to date. Skipping update.")
                    return True
                else:
                    logger.debug(f"Daily history data for item_id {item_id} is out of date. Most recent available data is dated at {most_recent_remote_timestamp} whereas the latest in database is {most_recent_local_timestamp}. Will download partial history.")
                    start = (most_recent_local_timestamp + timedelta(days=1)).date()

        daily_data_opt = get_daily(item_id, start=start)
        if daily_data_opt is None:
            logger.error(f"Daily history data download returned None. Cannot update item_id {item_id} in database.")
            return False
        daily_data = daily_data_opt

        self._write_daily(daily_data)

        return True

    def update_dailies(self, item_ids: int, chunk_size: int = 20) -> bool:
        """Download and write missing daily data for each ID in item_ids.

        Return false when unsuccessful.

        Idempotent."""

        # TODO: Squash this into a single SQL query
        most_recent_local_timestamps = self._most_recent_local_daily_timestamps(item_ids)

        item_ids_not_in_db = [key for key in most_recent_local_timestamps if most_recent_local_timestamps[key] is None]
        if (len(item_ids_not_in_db) > 0):
            logger.debug(f"{len(item_ids_not_in_db)} item IDs are missing from the database. Will download full history with individual requests.")

            for i, item_id in enumerate(item_ids_not_in_db):
                most_recent_local_timestamp = most_recent_local_timestamps[item_id]
                self.update_daily(item_id, full_download=True)

        item_ids_in_db = [key for key in most_recent_local_timestamps if most_recent_local_timestamps[key] is not None]
        if (len(item_ids_in_db) == 0):
            return True

        sublist_count = math.ceil(len(item_ids_in_db) / chunk_size)
        most_recent_remote_timestamp = self._most_recent_remote_daily_timestamp()
        logger.debug(f"Most recent remote data is dated {most_recent_remote_timestamp}")
        for item_ids in [item_ids_in_db[i*chunk_size:(i+1)*chunk_size] for i in range(sublist_count)]:
            self._update_dailies(item_ids, most_recent_local_timestamps, most_recent_remote_timestamp)

        self.conn.commit()

        return True

    def _update_dailies(self, item_ids: int, most_recent_local_timestamps: dict[int, datetime], most_recent_remote_timestamp: datetime) -> bool:
        """TODO"""

        oldest_most_recent_local_timestamp = self._oldest_timestamp([most_recent_local_timestamps[item_id] for item_id in item_ids])
        if oldest_most_recent_local_timestamp > most_recent_remote_timestamp:
            logger.debug(f"Daily history data for all item_ids ({item_ids}) is more recent than most-recent remote data. Skipping update.")
            return True

        if oldest_most_recent_local_timestamp == most_recent_remote_timestamp:
            logger.debug(f"Daily history data for all item_ids ({item_ids}) are up to date. Skipping update.")
            return True

        logger.debug(f"Daily history data is out of date. Most recent available data is dated at {most_recent_remote_timestamp} whereas the oldest most-recent in database is {oldest_most_recent_local_timestamp}. Will download partial history for all item ids ({item_ids}).")
        start = (oldest_most_recent_local_timestamp + timedelta(days=1)).date()

        daily_entries = get_dailies(item_ids, start=start)
        if daily_entries is None:
            logger.error(f"Daily history data download returned None. Cannot update item_ids ({item_ids}).")
            return False

        for item_id, entries in daily_entries.items():
            most_recent_local_timestamp_opt = self._most_recent_local_daily_timestamp(item_id)
            if most_recent_local_timestamp_opt is None:
                entries_to_write = entries
            else:
                most_recent_local_timestamp = most_recent_local_timestamp_opt
                entries_to_write = [entry for entry in entries if entry.utc_timestamp > most_recent_local_timestamp]

            self._write_daily(entries_to_write, commit=False)


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
            self.update_dailies(item_ids)

        logger.debug("Querying database")
        rows_opt = self._execute(f"SELECT * FROM daily_history WHERE id IN ({','.join(list(map(str, item_ids)))}) ORDER BY utc_timestamp ASC")
        if rows_opt is None:
            logger.debug(f"Database returned no rows")
            return None
        rows = rows_opt
        logger.debug(f"Done querying database")

        dailies = {}
        logger.debug(f"Parsing {len(rows)} database rows")
        for entry in list(map(row_to_history_entry, rows)):
            if not entry.id in dailies:
                dailies[entry.id] = []
            dailies[entry.id] += [entry]
        logger.debug(f"Done parsing {len(rows)} database rows")

        return dailies

    def _daily_history_ids(self, ):
        """Return list of unique IDs in daily_history table."""

        result = self.conn.cursor().execute(f"SELECT DISTINCT id FROM daily_history")


    def _write_items(self, entries: List[ItemEntry]) -> None:
        """Write given items to database."""

        # TODO: Replace "items" with variable
        self._insert_many("items", list(map(item_entry_to_tuple, entries)))

    def _write_daily(self, entries: List[HistoryEntry], commit: bool = True) -> None:
        """Write given daily history data to database."""

        # TODO: Replace "daily_history" with variable
        self._insert_many("daily_history", list(map(history_entry_to_tuple, entries)), commit)

    def _items_table_populated(self) -> bool:
        """Return true if the items table has any rows."""

        result = self._execute("SELECT COUNT(*) FROM items LIMIT 1")
        if result is None or result[0][0] is None:
            logger.debug(f"Items table is empty")
            return False

        return True

    def _most_recent_local_daily_timestamp(self, item_id: int) -> Optional[datetime]:
        """Return most recent timestamp for item_id in daily table.

        Returns None if item_id is absent from the table."""

        # TODO: Replace "daily_history" with variable
        result = self._execute(f"SELECT MAX(utc_timestamp) FROM daily_history WHERE id = {item_id}")
        if result is None or result[0][0] is None:
            logger.debug(f"No daily history data found for item_id {item_id}")
            return None
        most_recent_timestamp = result[0][0]
        most_recent_datetime = datetime.fromtimestamp(most_recent_timestamp, tz=timezone.utc)

        logger.debug(f"Most recent daily history data for item_id {item_id} is from {most_recent_datetime.isoformat()} ({most_recent_timestamp})")

        return most_recent_datetime

    def _most_recent_local_daily_timestamps(self, item_ids: List[int]) -> dict[int, Optional[datetime]]:
        """Return most recent timestamp for item_id in daily table.

        Returns None if item_id is absent from the table."""

        most_recent_timestamps = {}
        for item_id in item_ids:
            most_recent_timestamps[item_id] = self._most_recent_local_daily_timestamp(item_id)

        return most_recent_timestamps

    def _oldest_timestamp(self, timestamps: List[datetime]) -> datetime:
        """Return oldest timestamp."""

        oldest_timestamp = pytz.utc.localize(datetime.max)
        for timestamp in timestamps:
            if timestamp < oldest_timestamp:
                oldest_timestamp = timestamp

        return oldest_timestamp

    def _execute(self, query: str) -> Optional[List[tuple]]:
        """Execute given query and return results if present."""

        result = self.conn.cursor().execute(query)
        if result is None:
            return None

        return result.fetchall()

    def _insert_many(self, table_name: str, rows: List[tuple], commit: bool = True) -> None:
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
        if commit:
            self.conn.commit()
            logger.debug(f"Inserted {len(rows)} rows into {table_name}")

    def _most_recent_remote_daily_timestamp(self) -> Optional[datetime]:
        """Return most recent timestamp from server."""

        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        entries_opt = get_daily(glob_of_ectoplasm_id, start=yesterday.date())
        if entries_opt is None or len(entries_opt) < 1:
            logger.error(f"Daily history data download returned None or empty. Cannot determine most recent daily data.")
            return False
        return sorted(entries_opt, key=lambda entry: entry.utc_timestamp)[-1].utc_timestamp
