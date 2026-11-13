import sqlite3
import tempfile
import unittest
from pathlib import Path

from forexai.supervisor import TaskSupervisor


class SupervisorTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "tasks.sqlite3"
        self.supervisor = TaskSupervisor(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def test_registers_task(self):
        self.supervisor.register("health_check", 60)
        tasks = self.supervisor.status()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["name"], "health_check")

    def test_runs_due_task(self):
        self.supervisor.register("health_check", 60)
        results = self.supervisor.run_due({
            "health_check": lambda: "healthy"
        })
        self.assertEqual(results[0]["status"], "succeeded")
        self.assertEqual(results[0]["result"], "healthy")

    def test_requires_operator_confirmation_for_reset(self):
        self.supervisor.register("failing_task", 60, max_attempts=1)
        self.supervisor.run_due({"failing_task": lambda: 1 / 0})

        with self.assertRaises(PermissionError):
            self.supervisor.reset_failed(
                "failing_task", operator_confirmed=False
            )

    def test_does_not_persist_exception_message(self):
        self.supervisor.register("failing_task", 60, max_attempts=1)

        def failing_handler():
            raise RuntimeError("DUMMY_SECRET_VALUE")

        self.supervisor.run_due({"failing_task": failing_handler})

        task = self.supervisor.status()[0]
        self.assertNotIn("DUMMY_SECRET_VALUE", task["last_error"])
        self.assertEqual(task["status"], "failed")

    def test_recovers_interrupted_task(self):
        self.supervisor.register("health_check", 60)

        db = sqlite3.connect(self.db)
        try:
            db.execute(
                "UPDATE tasks SET status='running', attempts=1 "
                "WHERE name='health_check'"
            )
            db.commit()
        finally:
            db.close()

        recovered = self.supervisor.recover_after_restart()
        self.assertEqual(recovered, 1)
        self.assertEqual(self.supervisor.status()[0]["status"], "retry_wait")


if __name__ == "__main__":
    unittest.main()
