import os
import sqlite3

def create_tables(conn: sqlite3.Connection):
    """Create a new SQLite database at db_path.

    Keeps the connection open."""

    with open(os.path.join(os.path.dirname(__file__), 'schema.sql') , "r") as f:
        schema = f.read()
        conn.cursor().executescript(schema)
        conn.commit()

def connect(db_path: str):
    should_create_tables = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    return conn
