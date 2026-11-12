"""Process ownership and crash-recovery tests.

These tests use only the standard library (plus the project's own modules) so
they run in environments where Flask is unavailable.  They simulate two service
processes sharing one data directory by registering two process rows with
independent heartbeats.
"""

from __future__ import annotations

import gzip
import logging
import sqlite3
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ownership
from publish import (
    PublishError,
    _current_target,
    create_release,
    snapshot_for_sample,
)

LOGGER = logging.getLogger("test.ownership")
LOGGER.addHandler(logging.NullHandler())


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _make_work(work: Path, label: str = "one") -> None:
    work.mkdir(parents=True, exist_ok=True)
    with open(work / "R1.fastq.gz", "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            gz.write(f"@p1/1 {label}\nAC\n+\nII\n".encode())
    with open(work / "R2.fastq.gz", "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            gz.write(f"@p1/2 {label}\nGT\n+\nII\n".encode())


class OwnershipTest(unittest.TestCase):
    STALE = 30.0

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.db_path = root / "ingest.sqlite3"
        self.packages = root / "packages"
        self.staging = root / "staging"
        self.staging.mkdir()
        self.packages.mkdir()
        self.conn = _connect(self.db_path)
        ownership.init_schema(self.conn)
        self.process_a = "a" * 32
        self.process_b = "b" * 32
        ownership.register_process(self.conn, self.process_a)
        ownership.register_process(self.conn, self.process_b)

    def tearDown(self) -> None:
        self.conn.close()
        self._tmp.cleanup()

    # -- helpers -------------------------------------------------------------

    def _insert_job(
        self,
        sample_id: str = "SAMPLE-1",
        owner: str | None = None,
        status: str = "queued",
        job_id: str | None = None,
    ) -> str:
        job_id = job_id or uuid.uuid4().hex
        self.conn.execute(
            """
            INSERT INTO jobs(id, sample_id, status, stage, progress,
                             created_at, owner_id)
            VALUES (?, ?, ?, '排队中', 0, ?, ?)
            """,
            (job_id, sample_id, status, ownership.utc_now_iso(), owner),
        )
        self.conn.commit()
        return job_id

    def _stale_heartbeat(self, process_id: str, seconds: float) -> None:
        stale = (
            datetime.now(timezone.utc) - timedelta(seconds=seconds)
        ).isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE processes SET heartbeat_at = ? WHERE process_id = ?",
            (stale, process_id),
        )
        self.conn.commit()

    def _job(self, job_id: str) -> sqlite3.Row:
        return self.conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()

    def _reap(self, **kwargs):
        kwargs.setdefault("stale_seconds", self.STALE)
        return ownership.reap_jobs(
            self.conn,
            packages_root=self.packages,
            staging_root=self.staging,
            logger=LOGGER,
            **kwargs,
        )

    # -- liveness protection -------------------------------------------------

    def test_live_owner_jobs_are_not_touched_by_other_process(self) -> None:
        job_id = self._insert_job(owner=self.process_a, status="running")
        staging_dir = self.staging / job_id
        staging_dir.mkdir()
        (staging_dir / "input").write_bytes(b"busy")

        reclaimed = self._reap()
        self.assertEqual(reclaimed, [])
        row = self._job(job_id)
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["owner_id"], self.process_a)
        # Staging files of the live job survive.
        self.assertTrue((staging_dir / "input").exists())

    def test_other_samples_remain_uploadable_while_one_job_runs(self) -> None:
        running = self._insert_job(sample_id="BUSY", owner=self.process_a)
        # The unique partial index only covers queued/running rows.
        other = self._insert_job(sample_id="OTHER", owner=self.process_b)
        self.assertEqual(self._job(running)["status"], "queued")
        self.assertEqual(self._job(other)["status"], "queued")

        # A second upload of the busy sample is still rejected by the index.
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_job(sample_id="BUSY", owner=self.process_b)

    def test_dead_owner_jobs_are_interrupted_and_reupload_allowed(self) -> None:
        job_id = self._insert_job(owner=self.process_a)
        staging_dir = self.staging / job_id
        staging_dir.mkdir()
        (staging_dir / "R1.fastq.gz").write_bytes(b"partial")
        self._stale_heartbeat(self.process_a, self.STALE + 5)

        reclaimed = self._reap()
        self.assertEqual(reclaimed, [job_id])
        row = self._job(job_id)
        self.assertEqual(row["status"], "interrupted")
        self.assertIsNone(row["owner_id"])
        self.assertIn("中断", row["error"])
        self.assertFalse(staging_dir.exists())

        # The sample slot is free: a replacement upload can be inserted.
        replacement = self._insert_job(owner=self.process_b)
        self.assertEqual(self._job(replacement)["status"], "queued")
        self.assertEqual(self._job(replacement)["owner_id"], self.process_b)

    def test_deregistered_process_jobs_are_reclaimed_immediately(self) -> None:
        job_id = self._insert_job(owner=self.process_a)
        ownership.deregister_process(self.conn, self.process_a)
        reclaimed = self._reap()
        self.assertEqual(reclaimed, [job_id])
        self.assertEqual(self._job(job_id)["status"], "interrupted")
        # Process B's jobs are untouched because its heartbeat is fresh.
        other = self._insert_job(sample_id="KEEP", owner=self.process_b)
        self._reap()
        self.assertEqual(self._job(other)["status"], "queued")

    def test_reaper_only_cleans_incomplete_releases_of_dead_samples(self) -> None:
        # Live owner is building a half-finished release for SAMPLE-LIVE.
        live_job = self._insert_job(sample_id="SAMPLE-LIVE", owner=self.process_a)
        live_release = (
            self.packages / "SAMPLE-LIVE" / "releases" / "half-built"
        )
        live_release.mkdir(parents=True)
        (live_release / "R1.fastq.gz").write_bytes(b"partial")
        tmp_link = self.packages / "SAMPLE-LIVE" / ".current-abc.tmp"
        tmp_link.symlink_to(Path("releases") / "half-built", target_is_directory=True)

        # Dead owner left a half-finished release for SAMPLE-DEAD.
        dead_job = self._insert_job(sample_id="SAMPLE-DEAD", owner=self.process_b)
        dead_release = (
            self.packages / "SAMPLE-DEAD" / "releases" / "half-built"
        )
        dead_release.mkdir(parents=True)
        (dead_release / "R1.fastq.gz").write_bytes(b"partial")
        self._stale_heartbeat(self.process_b, self.STALE + 5)

        self._reap()
        # Live process's workspace is fully preserved.
        self.assertTrue(live_release.exists())
        self.assertTrue(tmp_link.exists())
        self.assertEqual(self._job(live_job)["status"], "queued")
        # Dead process's incomplete release is removed and its job interrupted.
        self.assertFalse(dead_release.exists())
        self.assertEqual(self._job(dead_job)["status"], "interrupted")

    def test_completed_package_recovered_as_success_after_owner_dies(self) -> None:
        job_id = self._insert_job(owner=self.process_a, status="running")
        work = self.staging / job_id / "output"
        _make_work(work, "recover")
        snapshot = create_release(
            work_dir=work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id=job_id,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
        )
        self._stale_heartbeat(self.process_a, self.STALE + 5)

        reclaimed = self._reap()
        self.assertEqual(reclaimed, [job_id])
        row = self._job(job_id)
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["release_id"], snapshot.release_id)
        self.assertIsNone(row["error"])
        self.assertIsNone(row["owner_id"])
        pkg = self.conn.execute(
            "SELECT * FROM packages WHERE sample_id = 'SAMPLE-1'"
        ).fetchone()
        self.assertEqual(pkg["release_id"], snapshot.release_id)
        # The published package is still downloadable from disk.
        self.assertIsNotNone(snapshot_for_sample(self.packages, "SAMPLE-1"))

    def test_package_belonging_to_newer_job_does_not_resurrect_old_job(self) -> None:
        old_job = self._insert_job(sample_id="SAMPLE-1", owner=self.process_a)
        # A newer retry (different job id) published its package before the old
        # owner's row is reclaimed.
        work = self.staging / "new-output"
        _make_work(work, "new")
        newer = create_release(
            work_dir=work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id="f" * 32,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
        )
        self._stale_heartbeat(self.process_a, self.STALE + 5)
        self._reap()
        row = self._job(old_job)
        self.assertEqual(row["status"], "interrupted")
        self.assertIsNone(row["release_id"])
        # The newer published package is what remains live.
        self.assertEqual(
            snapshot_for_sample(self.packages, "SAMPLE-1").release_id,
            newer.release_id,
        )

    # -- fencing -------------------------------------------------------------

    def test_worker_writes_are_ignored_after_reclaim(self) -> None:
        job_id = self._insert_job(owner=self.process_a)
        self.assertTrue(
            ownership.claim_running(
                self.conn, job_id, self.process_a, ownership.utc_now_iso()
            )
        )
        self.assertTrue(
            ownership.write_progress(
                self.conn, job_id, self.process_a, "校验 R1", 10, 1, 0
            )
        )
        # Simulate reclaim by another process.
        self._stale_heartbeat(self.process_a, self.STALE + 5)
        self._reap()
        self.assertEqual(self._job(job_id)["status"], "interrupted")

        self.assertFalse(
            ownership.write_progress(
                self.conn, job_id, self.process_a, "校验 R1", 20, 2, 0
            )
        )
        self.assertFalse(
            ownership.fail_job(self.conn, job_id, self.process_a, "迟到的失败")
        )
        row = self._job(job_id)
        self.assertEqual(row["status"], "interrupted")
        self.assertNotIn("迟到的失败", row["error"])

    def test_stale_worker_cannot_complete_reclaimed_job(self) -> None:
        job_id = self._insert_job(owner=self.process_a)
        self._stale_heartbeat(self.process_a, self.STALE + 5)
        self._reap()

        work = self.staging / "late-output"
        _make_work(work, "late")
        snapshot = create_release(
            work_dir=work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id=job_id,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
        )
        self.assertFalse(
            ownership.complete_job(
                self.conn,
                job_id,
                self.process_a,
                snapshot,
                1,
                ownership.utc_now_iso(),
            )
        )
        # The terminal row survives and the stale completion did not flip it.
        self.assertEqual(self._job(job_id)["status"], "interrupted")

    def test_publication_fence_refuses_to_overwrite_newer_package(self) -> None:
        # First task publishes a package.
        first_work = self.staging / "first"
        _make_work(first_work, "first")
        first = create_release(
            work_dir=first_work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id="1" * 32,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
        )
        target_before = _current_target(self.packages / "SAMPLE-1")
        self.assertEqual(
            target_before, str(Path("releases") / first.release_id)
        )

        # A newer task publishes while the old worker still holds a stale
        # expectation pointing at the first release.
        second_work = self.staging / "second"
        _make_work(second_work, "second")
        second = create_release(
            work_dir=second_work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id="2" * 32,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
        )

        stale_work = self.staging / "stale"
        _make_work(stale_work, "stale")
        with self.assertRaisesRegex(PublishError, "不得覆盖新包"):
            create_release(
                work_dir=stale_work,
                packages_root=self.packages,
                sample_id="SAMPLE-1",
                job_id="1" * 32,
                read_count=1,
                r1_sha256="h1",
                r2_sha256="h2",
                keep_releases=2,
                expected_current=target_before,
            )
        # The newer package is untouched and downloadable.
        live = snapshot_for_sample(self.packages, "SAMPLE-1")
        self.assertEqual(live.release_id, second.release_id)

    def test_publication_fence_allows_first_publish_and_expected_replacement(self):
        work = self.staging / "only"
        _make_work(work, "only")
        snapshot = create_release(
            work_dir=work,
            packages_root=self.packages,
            sample_id="FRESH",
            job_id="3" * 32,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
            expected_current=None,
        )
        self.assertIsNotNone(snapshot)
        target = _current_target(self.packages / "FRESH")

        work2 = self.staging / "only-2"
        _make_work(work2, "again")
        snapshot2 = create_release(
            work_dir=work2,
            packages_root=self.packages,
            sample_id="FRESH",
            job_id="4" * 32,
            read_count=1,
            r1_sha256="h1",
            r2_sha256="h2",
            keep_releases=2,
            expected_current=target,
        )
        self.assertEqual(
            _current_target(self.packages / "FRESH"),
            str(Path("releases") / snapshot2.release_id),
        )

    def test_unknown_staging_is_swept_only_after_orphan_age(self) -> None:
        old_orphan = self.staging / "orphan-old"
        old_orphan.mkdir()
        (old_orphan / "blob").write_bytes(b"x")
        old_time = time.time() - 7200
        import os as _os

        _os.utime(old_orphan, (old_time, old_time))
        new_orphan = self.staging / "orphan-new"
        new_orphan.mkdir()

        job_id = self._insert_job(owner=self.process_a)
        job_dir = self.staging / job_id
        job_dir.mkdir()

        ownership.sweep_unknown_staging(
            self.conn, self.staging, 3600.0, LOGGER
        )
        self.assertFalse(old_orphan.exists())
        self.assertTrue(new_orphan.exists())
        self.assertTrue(job_dir.exists())


if __name__ == "__main__":
    unittest.main()
