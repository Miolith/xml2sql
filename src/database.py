import sqlite3
import logging
import os

def reset_db(database):
    if os.path.exists(database):
        os.remove(database)


"""
Create a new SQLite database from a path to a .db file
and returns a connection to it
"""
def connect_to_db(database: str) -> sqlite3.Connection:
    reset_db(database)
    con = sqlite3.connect(database)
    con.row_factory = sqlite3.Row
    logger = logging.getLogger()
    con.set_trace_callback(logger.debug)
    return con
