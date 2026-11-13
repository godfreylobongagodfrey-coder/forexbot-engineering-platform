import json
import tempfile
import unittest
from pathlib import Path

from forexai.audit import record


class AuditTests(unittest.TestCase):

    def test_accepts_normal_operational_event(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"

            record(path, {
                "event": "MARKET_OBSERVATION",
                "status": "success",
                "records_processed": 25,
            })

            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(data["event"], "MARKET_OBSERVATION")
            self.assertEqual(data["status"], "success")

    def test_rejects_password_field(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"

            with self.assertRaises(ValueError):
                record(path, {
                    "event": "TEST",
                    "password": "DUMMY_ONLY",
                })

    def test_rejects_nested_api_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"

            with self.assertRaises(ValueError):
                record(path, {
                    "event": "TEST",
                    "details": {
                        "api_key": "DUMMY_ONLY",
                    },
                })


if __name__ == "__main__":
    unittest.main()
