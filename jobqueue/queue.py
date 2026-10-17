"""A SQLite-backed job queue with leases, retries, and dead-lettering. Standard library only.

A worker claims a job for a lease period. If it does not finish before the lease ends, the job
goes back to the queue (or to dead, once its attempts are used up). Each claim returns an attempt
number, and only the holder of the latest attempt, with a lease that has not ended, can complete
or fail the job. A stale worker therefore cannot overwrite a newer one.
"""

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class Job:
    id: int
    payload: dict
    attempt: int


class Queue:
    def __init__(self, path: str = ":memory:", clock: Callable[[], float] = time.time):
        self.conn = sqlite3.connect(path, isolation_level=None)  # explicit transactions below
        self.clock = clock
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " payload TEXT NOT NULL,"
            " status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'done', 'dead')),"
            " attempts INTEGER NOT NULL DEFAULT 0,"
            " max_attempts INTEGER NOT NULL,"
            " lease_until REAL,"
            " last_error TEXT)"
        )

    def enqueue(self, payload: dict, max_attempts: int = 3) -> int:
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
            raise ValueError("max_attempts must be an integer of at least 1")
        cur = self.conn.execute(
            "INSERT INTO jobs (payload, status, max_attempts) VALUES (?, 'queued', ?)",
            (json.dumps(payload), max_attempts),
        )
        return cur.lastrowid

    def claim(self, lease_seconds: float = 60) -> Optional[Job]:
        """Give the next queued job to a worker, or return None if there is none."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = self.clock()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self._expire_leases(now)
            row = self.conn.execute(
                "SELECT id, payload FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                self.conn.execute("COMMIT")
                return None
            job_id, payload = row
            self.conn.execute(
                "UPDATE jobs SET status = 'running', attempts = attempts + 1, lease_until = ?"
                " WHERE id = ?",
                (now + lease_seconds, job_id),
            )
            attempt = self.conn.execute("SELECT attempts FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]
            self.conn.execute("COMMIT")
            return Job(id=job_id, payload=json.loads(payload), attempt=attempt)
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def _expire_leases(self, now: float) -> None:
        self.conn.execute(
            "UPDATE jobs SET"
            " status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,"
            " last_error = 'lease expired',"
            " lease_until = NULL"
            " WHERE status = 'running' AND lease_until < ?",
            (now,),
        )

    def complete(self, job_id: int, attempt: int) -> bool:
        """Mark a job done. Returns False if this attempt no longer holds a live lease."""
        cur = self.conn.execute(
            "UPDATE jobs SET status = 'done', lease_until = NULL, last_error = NULL"
            " WHERE id = ? AND attempts = ? AND status = 'running' AND lease_until >= ?",
            (job_id, attempt, self.clock()),
        )
        return cur.rowcount == 1

    def fail(self, job_id: int, attempt: int, error: str) -> Optional[str]:
        """Record a failure. Returns the new status (queued or dead), or None if this attempt holds no live lease."""
        cur = self.conn.execute(
            "UPDATE jobs SET"
            " status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,"
            " lease_until = NULL, last_error = ?"
            " WHERE id = ? AND attempts = ? AND status = 'running' AND lease_until >= ?",
            (error, job_id, attempt, self.clock()),
        )
        if cur.rowcount != 1:
            return None
        return self.conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]

    def status(self, job_id: int) -> Optional[dict]:
        """Current state. A running job whose lease has ended reports 'expired' until the next claim sweeps it."""
        row = self.conn.execute(
            "SELECT status, attempts, max_attempts, last_error, lease_until FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        state, attempts, max_attempts, last_error, lease_until = row
        if state == "running" and lease_until < self.clock():
            state = "expired"
        return {"status": state, "attempts": attempts, "max_attempts": max_attempts, "last_error": last_error}
