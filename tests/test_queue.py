import unittest

from jobqueue.queue import Queue


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.q = Queue(clock=self.clock)

    def test_claims_in_order_and_each_job_once(self):
        a = self.q.enqueue({"n": 1})
        b = self.q.enqueue({"n": 2})
        first = self.q.claim()
        second = self.q.claim()
        self.assertEqual((first.id, second.id), (a, b))
        self.assertIsNone(self.q.claim())

    def test_complete_finishes_the_job(self):
        job_id = self.q.enqueue({"n": 1})
        job = self.q.claim()
        self.assertTrue(self.q.complete(job.id, job.attempt))
        self.assertEqual(self.q.status(job_id)["status"], "done")
        self.assertIsNone(self.q.claim())

    def test_expired_lease_goes_back_to_queue(self):
        job_id = self.q.enqueue({"n": 1}, max_attempts=3)
        self.q.claim(lease_seconds=60)
        self.clock.now += 61
        again = self.q.claim(lease_seconds=60)
        self.assertEqual(again.id, job_id)
        self.assertEqual(again.attempt, 2)

    def test_stale_worker_cannot_complete_a_reclaimed_job(self):
        self.q.enqueue({"n": 1}, max_attempts=3)
        stale = self.q.claim(lease_seconds=60)
        self.clock.now += 61
        current = self.q.claim(lease_seconds=60)
        self.assertFalse(self.q.complete(stale.id, stale.attempt))
        self.assertTrue(self.q.complete(current.id, current.attempt))

    def test_failures_retry_then_dead_letter(self):
        job_id = self.q.enqueue({"n": 1}, max_attempts=2)
        first = self.q.claim()
        self.assertEqual(self.q.fail(first.id, first.attempt, "boom"), "queued")
        second = self.q.claim()
        self.assertEqual(self.q.fail(second.id, second.attempt, "boom again"), "dead")
        self.assertIsNone(self.q.claim())
        status = self.q.status(job_id)
        self.assertEqual((status["status"], status["attempts"], status["last_error"]),
                         ("dead", 2, "boom again"))

    def test_lease_expiry_counts_toward_dead_letter(self):
        job_id = self.q.enqueue({"n": 1}, max_attempts=2)
        self.q.claim(lease_seconds=10)
        self.clock.now += 11
        self.q.claim(lease_seconds=10)
        self.clock.now += 11
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.status(job_id)["status"], "dead")

    def test_invalid_max_attempts_rejected(self):
        with self.assertRaises(ValueError):
            self.q.enqueue({"n": 1}, max_attempts=0)


if __name__ == "__main__":
    unittest.main()
