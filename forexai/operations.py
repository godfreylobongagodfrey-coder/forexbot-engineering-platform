from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path


def local_health_snapshot(
    project_root: str | Path,
    *,
    observation_db_path: str | Path | None = None,
    min_free_bytes: int = 2 * 1024**3,
) -> dict:
    """Return a read-only health snapshot of a local application environment."""

    if min_free_bytes < 0:
        raise ValueError("Minimum free-space threshold cannot be negative")

    root = Path(project_root).resolve()
    root_accessible = root.is_dir() and os.access(root, os.R_OK | os.X_OK)

    disk_free = shutil.disk_usage(root).free if root_accessible else None
    disk_ok = disk_free is not None and disk_free >= min_free_bytes

    observation = {
        "status": "not_configured",
        "integrity_verified": None,
    }

    if observation_db_path is not None:
        database = Path(observation_db_path).resolve()

        if not database.is_file():
            observation = {
                "status": "missing",
                "integrity_verified": False,
            }
        else:
            connection = None
            try:
                uri = database.as_uri() + "?mode=ro"
                connection = sqlite3.connect(uri, uri=True, timeout=2)
                result = connection.execute("PRAGMA quick_check").fetchone()
                valid = result is not None and result[0] == "ok"

                observation = {
                    "status": "healthy" if valid else "integrity_failed",
                    "integrity_verified": valid,
                }
            except (sqlite3.Error, OSError, ValueError):
                observation = {
                    "status": "corrupt_or_unreadable",
                    "integrity_verified": False,
                }
            finally:
                if connection is not None:
                    connection.close()

    checks = {
        "project_root_accessible": root_accessible,
        "disk_free_above_threshold": disk_ok,
        "observation_store_integrity": observation["integrity_verified"],
    }

    healthy = all(value for value in checks.values() if value is not None)

    return {
        "status": "healthy" if healthy else "attention_required",
        "checks": checks,
        "disk_free_bytes": disk_free,
        "observation_store": observation,
        "read_only": True,
    }
