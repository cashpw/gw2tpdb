import os
import sqlite3

def create_database(conn):
    """Create a new SQLite database at db_path.

    Keeps the connection open."""

    with open("schema.sql", "r") as f:
        schema = f.read()
        conn.cursor().executescript(schema)
        conn.commit()

def connect(db_path: str):
    should_create_tables = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    if should_create_tables:
        create_tables(db)

    return conn
