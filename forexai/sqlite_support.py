"""SQLite session that commits/rolls back and explicitly closes on Windows."""
from contextlib import contextmanager
import sqlite3


@contextmanager
def sqlite_session(path,*,timeout=5.0,isolation_level="DEFERRED",row_factory=None,synchronous_full=False):
    db=sqlite3.connect(path,timeout=timeout,isolation_level=isolation_level)
    if row_factory is not None: db.row_factory=row_factory
    try:
        if synchronous_full: db.execute("PRAGMA synchronous=FULL")
        with db:
            yield db
    finally:
        db.close()
