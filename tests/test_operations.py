import sqlite3
import tempfile
import unittest
from pathlib import Path

from forexai.operations import local_health_snapshot


class OperationsTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_reports_accessible_project_root(self):
        report = local_health_snapshot(self.root, min_free_bytes=0)
        self.assertTrue(report["checks"]["project_root_accessible"])
        self.assertEqual(report["status"], "healthy")

    def test_reports_disk_space(self):
        report = local_health_snapshot(self.root, min_free_bytes=0)
        self.assertIsInstance(report["disk_free_bytes"], int)
        self.assertTrue(report["checks"]["disk_free_above_threshold"])

    def test_detects_missing_observation_database(self):
        report = local_health_snapshot(
            self.root,
            observation_db_path=self.root / "missing.sqlite3",
            min_free_bytes=0,
        )
        self.assertEqual(report["observation_store"]["status"], "missing")
        self.assertFalse(report["checks"]["observation_store_integrity"])

    def test_checks_database_integrity_read_only(self):
        database = self.root / "observations.sqlite3"

        connection = sqlite3.connect(database)
        try:
            connection.execute(
                "CREATE TABLE observations (id INTEGER PRIMARY KEY)"
            )
            connection.execute(
                "INSERT INTO observations DEFAULT VALUES"
            )
            connection.commit()
        finally:
            connection.close()

        report = local_health_snapshot(
            self.root,
            observation_db_path=database,
            min_free_bytes=0,
        )

        self.assertEqual(report["observation_store"]["status"], "healthy")
        self.assertTrue(report["checks"]["observation_store_integrity"])
        self.assertTrue(report["read_only"])

    def test_rejects_negative_disk_threshold(self):
        with self.assertRaises(ValueError):
            local_health_snapshot(self.root, min_free_bytes=-1)


if __name__ == "__main__":
    unittest.main()
