"""A SQLite-backed job queue with leases, retries, and dead-lettering. Standard library only.

A worker claims a job for a lease period. If it does not finish in time, the job goes back
to the queue. Each claim returns an attempt number, and only the holder of the latest attempt
can complete or fail the job, so a stale worker cannot overwrite a newer one.
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
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        cur = self.conn.execute(
            "INSERT INTO jobs (payload, status, max_attempts) VALUES (?, 'queued', ?)",
            (json.dumps(payload), max_attempts),
        )
        return cur.lastrowid

    def claim(self, lease_seconds: float = 60) -> Optional[Job]:
        """Give the next queued job to a worker, or return None if there is none."""
        now = self.clock()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            # Jobs whose lease ran out go back to the queue, or to dead if no attempts are left.
            self.conn.execute(
                "UPDATE jobs SET"
                " status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,"
                " last_error = COALESCE(last_error, 'lease expired')"
                " WHERE status = 'running' AND lease_until < ?",
                (now,),
            )
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

    def complete(self, job_id: int, attempt: int) -> bool:
        """Mark a job done. Returns False if this attempt no longer holds the job."""
        cur = self.conn.execute(
            "UPDATE jobs SET status = 'done', lease_until = NULL"
            " WHERE id = ? AND attempts = ? AND status = 'running'",
            (job_id, attempt),
        )
        return cur.rowcount == 1

    def fail(self, job_id: int, attempt: int, error: str) -> Optional[str]:
        """Record a failure. Returns the new status (queued or dead), or None if this attempt is stale."""
        row = self.conn.execute(
            "SELECT attempts, max_attempts FROM jobs WHERE id = ? AND status = 'running'", (job_id,)
        ).fetchone()
        if row is None or row[0] != attempt:
            return None
        new_status = "dead" if row[0] >= row[1] else "queued"
        self.conn.execute(
            "UPDATE jobs SET status = ?, lease_until = NULL, last_error = ? WHERE id = ?",
            (new_status, error, job_id),
        )
        return new_status

    def status(self, job_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT status, attempts, max_attempts, last_error FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        return {"status": row[0], "attempts": row[1], "max_attempts": row[2], "last_error": row[3]}
