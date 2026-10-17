import unittest

from jobqueue.queue import Queue


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FixTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.q = Queue(clock=self.clock)

    def test_non_positive_lease_is_rejected(self):
        self.q.enqueue({"n": 1})
        with self.assertRaises(ValueError):
            self.q.claim(lease_seconds=-1)

    def test_late_completion_after_lease_end_is_rejected(self):
        self.q.enqueue({"n": 1})
        job = self.q.claim(lease_seconds=60)
        self.clock.now += 61
        self.assertFalse(self.q.complete(job.id, job.attempt))

    def test_late_failure_after_lease_end_is_rejected(self):
        self.q.enqueue({"n": 1})
        job = self.q.claim(lease_seconds=60)
        self.clock.now += 61
        self.assertIsNone(self.q.fail(job.id, job.attempt, "late"))

    def test_expiry_overwrites_an_older_error(self):
        job_id = self.q.enqueue({"n": 1}, max_attempts=2)
        first = self.q.claim(lease_seconds=60)
        self.q.fail(job_id, first.attempt, "boom")
        self.q.claim(lease_seconds=60)
        self.clock.now += 61
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.status(job_id)["last_error"], "lease expired")

    def test_status_reports_expired_lease(self):
        job_id = self.q.enqueue({"n": 1})
        self.q.claim(lease_seconds=60)
        self.clock.now += 61
        self.assertEqual(self.q.status(job_id)["status"], "expired")

    def test_completion_clears_last_error(self):
        job_id = self.q.enqueue({"n": 1})
        first = self.q.claim()
        self.q.fail(job_id, first.attempt, "boom")
        second = self.q.claim()
        self.assertTrue(self.q.complete(job_id, second.attempt))
        self.assertIsNone(self.q.status(job_id)["last_error"])

    def test_payload_and_max_attempts_are_type_checked(self):
        with self.assertRaises(ValueError):
            self.q.enqueue([1])
        with self.assertRaises(ValueError):
            self.q.enqueue({"n": 1}, max_attempts=1.5)
        with self.assertRaises(ValueError):
            self.q.enqueue({"n": 1}, max_attempts=True)


if __name__ == "__main__":
    unittest.main()
