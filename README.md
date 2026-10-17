# job-queue

A small job queue in SQLite, with leases, retries, and dead-lettering. Standard library only.

- `enqueue(payload, max_attempts)` adds a job.
- `claim(lease_seconds)` hands the next job to a worker and returns an attempt number.
- `complete(id, attempt)` and `fail(id, attempt, error)` only work for the holder of the latest attempt, so a stale worker cannot overwrite a newer one.
- A job whose lease expires goes back to the queue, and counts as an attempt. After `max_attempts`, it becomes `dead`.

Single-process use. Clock injection makes lease behavior testable.

Run the tests: `python3 -m unittest -q tests.test_queue`
