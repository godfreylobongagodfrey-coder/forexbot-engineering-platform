import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from forexai.mt5_observation import ObservationStore


class ObservationStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "observations.sqlite3"
        self.store = ObservationStore(self.db_path)
        self.now = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_observation(self, timestamp):
        timestamp = timestamp.astimezone(timezone.utc)
        return {
            "broker": "TEST",
            "server": "TEST-DEMO",
            "pair": "EURUSD",
            "symbol": "EURUSD",
            "time_utc": timestamp,
            "collected_utc": timestamp,
            "bid": 1.10000,
            "ask": 1.10020,
            "bars": [],
        }

    def test_database_initializes_and_verifies(self):
        self.assertTrue(self.db_path.exists())
        self.assertTrue(self.store.verify())

    def test_records_operational_event(self):
        self.store.event(
            "test_event",
            {"component": "observation_store"},
            at=self.now,
        )
        self.assertTrue(self.store.verify())

    def test_rejects_out_of_order_observation(self):
        newer = self.make_observation(self.now.replace(minute=2))
        self.assertNotEqual(self.store.append(newer), "out_of_order")

        older = self.make_observation(self.now.replace(minute=1))
        self.assertEqual(self.store.append(older), "out_of_order")

    def test_rejects_duplicate_observation(self):
        observation = self.make_observation(self.now)
        first = self.store.append(observation)
        second = self.store.append(observation)

        self.assertNotEqual(first, "duplicate")
        self.assertEqual(second, "duplicate")

    def test_reports_database_health(self):
        result = self.store.health(now=self.now)
        self.assertIsInstance(result, dict)
        self.assertIn("status", result)

    def test_prevents_modification_of_stored_observation(self):
        self.store.append(self.make_observation(self.now))

        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE observations SET payload_json=?",
                    ('{"tampered": true}',),
                )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
