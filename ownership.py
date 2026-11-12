"""Process ownership, heartbeats and crash recovery for ingest jobs.

Every service process registers itself in the ``processes`` table and keeps a
heartbeat row fresh while it runs.  Each job records the id of the process that
accepted the upload (``jobs.owner_id``).  A job is only eligible for crash
recovery when its owner is no longer alive:

* the owner row is missing (the process deregistered on graceful shutdown), or
* its heartbeat is older than the configured staleness timeout (SIGKILL, host
  failure, ...).

As long as an owner heartbeats, no other process sharing the same data
directory may mark its queued/running jobs as interrupted, delete their staging
files or remove the releases they are building.  All state transitions made by
a worker are additionally fenced with ``owner_id`` predicates, so a process that
wakes up after being reclaimed cannot resurrect a terminal job or overwrite a
package published later by the replacement upload.

The module deliberately depends on the standard library only so that the
ownership/recovery logic is testable without Flask.
"""

from __future__ import annotations

import logging
import shutil
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from publish import (
    cleanup_incomplete,
    snapshot_for_sample,
    utc_now_iso,
)

LOGGER = logging.getLogger("sample_ingest.ownership")

ACTIVE_STATUSES = ("queued", "running")
INTERRUPTED_ERROR = "负责该任务的服务进程已退出，任务中断；可重新上传样本。"


class JobOwnershipLost(Exception):
    """Raised when a worker discovers another process reclaimed its job."""


def _to_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the schema and migrate older databases in place."""

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            sample_id TEXT NOT NULL,
            status TEXT NOT NULL,
            stage TEXT NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            r1_reads INTEGER NOT NULL DEFAULT 0,
            r2_reads INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            release_id TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        );

        CREATE TABLE IF NOT EXISTS packages (
            sample_id TEXT PRIMARY KEY,
            release_id TEXT NOT NULL,
            read_count INTEGER NOT NULL,
            r1_sha256 TEXT,
            r2_sha256 TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS processes (
            process_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_per_sample
            ON jobs(sample_id)
            WHERE status IN ('queued', 'running');

        CREATE INDEX IF NOT EXISTS jobs_sample_created
            ON jobs(sample_id, created_at);
        """
    )
    # Databases created before process ownership existed lack owner_id.  Rows
    # from that era stay NULL and are treated as belonging to a dead process.
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
    }
    if "owner_id" not in existing:
        conn.execute("ALTER TABLE jobs ADD COLUMN owner_id TEXT")
    conn.commit()


# ---------------------------------------------------------------------------
# Process registry
# ---------------------------------------------------------------------------


def register_process(conn: sqlite3.Connection, process_id: str) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO processes(process_id, started_at, heartbeat_at)
        VALUES (?, ?, ?)
        """,
        (process_id, now, now),
    )
    conn.commit()


def heartbeat(conn: sqlite3.Connection, process_id: str) -> bool:
    """Refresh the heartbeat. Returns False if the process was deregistered."""

    cursor = conn.execute(
        "UPDATE processes SET heartbeat_at = ? WHERE process_id = ?",
        (utc_now_iso(), process_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def deregister_process(conn: sqlite3.Connection, process_id: str) -> None:
    conn.execute("DELETE FROM processes WHERE process_id = ?", (process_id,))
    conn.commit()


def live_process_ids(
    conn: sqlite3.Connection, stale_seconds: float, now: datetime | None = None
) -> set[str]:
    if now is None:
        now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=float(stale_seconds))
    live: set[str] = set()
    for process_id, heartbeat_at in conn.execute(
        "SELECT process_id, heartbeat_at FROM processes"
    ).fetchall():
        try:
            if _parse_ts(heartbeat_at) >= cutoff:
                live.add(process_id)
        except (TypeError, ValueError):
            # An unreadable heartbeat cannot prove liveness: treat as dead.
            continue
    return live


# ---------------------------------------------------------------------------
# Ownership-fenced job mutations (used by the worker that owns the job)
# ---------------------------------------------------------------------------


def claim_running(
    conn: sqlite3.Connection, job_id: str, owner_id: str, started_at: str
) -> bool:
    cursor = conn.execute(
        """
        UPDATE jobs
           SET status = 'running', stage = '开始处理', progress = 1,
               started_at = ?, error = NULL
         WHERE id = ? AND status = 'queued' AND owner_id IS ?
        """,
        (started_at, job_id, owner_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def write_progress(
    conn: sqlite3.Connection,
    job_id: str,
    owner_id: str,
    stage: str,
    percent: int,
    r1_reads: int,
    r2_reads: int,
) -> bool:
    """Update progress. False means the job was reclaimed and work must stop."""

    cursor = conn.execute(
        """
        UPDATE jobs
           SET stage = ?, progress = ?, r1_reads = ?, r2_reads = ?
         WHERE id = ? AND status IN ('queued', 'running') AND owner_id IS ?
        """,
        (
            stage,
            max(0, min(100, int(percent))),
            r1_reads,
            r2_reads,
            job_id,
            owner_id,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def fail_job(
    conn: sqlite3.Connection,
    job_id: str,
    owner_id: str,
    error: str,
    stage: str = "失败",
) -> bool:
    finished = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
           SET status = 'failed', stage = ?, progress = 100, error = ?,
               finished_at = COALESCE(finished_at, ?)
         WHERE id = ? AND status IN ('queued', 'running') AND owner_id IS ?
        """,
        (stage, error, finished, job_id, owner_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def upsert_package(conn: sqlite3.Connection, snapshot) -> None:
    manifest = snapshot.manifest
    files = manifest.get("files") or {}
    r1 = files.get("R1") or {}
    r2 = files.get("R2") or {}
    conn.execute(
        """
        INSERT INTO packages(sample_id, release_id, read_count, r1_sha256,
                             r2_sha256, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(sample_id) DO UPDATE SET
            release_id = excluded.release_id,
            read_count = excluded.read_count,
            r1_sha256 = excluded.r1_sha256,
            r2_sha256 = excluded.r2_sha256,
            updated_at = excluded.updated_at
        """,
        (
            snapshot.sample_id,
            snapshot.release_id,
            _to_int(manifest.get("read_count")),
            r1.get("sha256"),
            r2.get("sha256"),
            manifest.get("created_at") or utc_now_iso(),
        ),
    )


def complete_job(
    conn: sqlite3.Connection,
    job_id: str,
    owner_id: str,
    snapshot,
    read_count: int,
    finished_at: str,
) -> bool:
    """Publish the success state behind an ownership fence.

    Returns False when the job no longer belongs to this worker.  In that case
    the worker must not touch the row; the reaper reconciles the final state
    from the on-disk package.
    """

    conn.execute("BEGIN IMMEDIATE")
    try:
        upsert_package(conn, snapshot)
        cursor = conn.execute(
            """
            UPDATE jobs
               SET status = 'succeeded', stage = '完成', progress = 100,
                   r1_reads = ?, r2_reads = ?, error = NULL,
                   release_id = ?, finished_at = ?
             WHERE id = ? AND status IN ('queued', 'running')
               AND owner_id IS ?
            """,
            (
                read_count,
                read_count,
                snapshot.release_id,
                finished_at,
                job_id,
                owner_id,
            ),
        )
        if cursor.rowcount == 0:
            conn.rollback()
            return False
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Recovery of jobs whose owner process is gone
# ---------------------------------------------------------------------------


def interrupt_job(
    conn: sqlite3.Connection, job_id: str, error: str = INTERRUPTED_ERROR
) -> None:
    """Mark an orphaned active job as interrupted and release the sample slot."""

    finished = utc_now_iso()
    conn.execute(
        """
        UPDATE jobs
           SET status = 'interrupted', stage = '中断', progress = 100,
               error = ?, finished_at = COALESCE(finished_at, ?),
               owner_id = NULL
         WHERE id = ? AND status IN ('queued', 'running')
        """,
        (error, finished, job_id),
    )


def _recover_succeeded_job(
    conn: sqlite3.Connection, job_id: str, snapshot
) -> None:
    manifest = snapshot.manifest
    finished = manifest.get("created_at") or utc_now_iso()
    upsert_package(conn, snapshot)
    conn.execute(
        """
        UPDATE jobs
           SET status = 'succeeded', stage = '完成', progress = 100,
               error = NULL, release_id = ?,
               started_at = COALESCE(started_at, ?), finished_at = ?,
               owner_id = NULL
         WHERE id = ? AND status IN ('queued', 'running')
        """,
        (snapshot.release_id, finished, finished, job_id),
    )


def _remove_staging(staging_root: Path, job_id: str, logger: logging.Logger) -> None:
    path = staging_root / job_id
    try:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    except OSError:
        logger.warning("无法清理中断任务的暂存目录：%s", path, exc_info=True)


def sweep_unknown_staging(
    conn: sqlite3.Connection,
    staging_root: Path,
    orphan_age_seconds: float,
    logger: logging.Logger,
    now: float | None = None,
) -> None:
    """Remove staging entries that belong to no known job and are old enough.

    A brand-new upload writes its files before its job row exists, so unknown
    entries are only removed after a generous age threshold; entries of live
    jobs are never touched here.
    """

    if not staging_root.is_dir() or orphan_age_seconds <= 0:
        return
    if now is None:
        now = time.time()
    known = {row[0] for row in conn.execute("SELECT id FROM jobs").fetchall()}
    for item in staging_root.iterdir():
        if item.name in known:
            continue
        try:
            age = now - item.stat().st_mtime
        except OSError:
            continue
        if age < orphan_age_seconds:
            continue
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        except OSError:
            logger.warning("无法清理孤立暂存条目：%s", item, exc_info=True)


def reap_jobs(
    conn: sqlite3.Connection,
    *,
    packages_root: Path,
    staging_root: Path,
    stale_seconds: float,
    orphan_age_seconds: float = 3600.0,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> list[str]:
    """Reclaim active jobs whose owner process is no longer alive.

    Jobs owned by a process with a fresh heartbeat are left completely
    untouched, including their staging directories and in-progress releases.
    Orphaned jobs are either recovered as succeeded (when their fully
    published package is found on disk) or marked interrupted so the sample can
    be re-uploaded.  Returns the reclaimed job ids.
    """

    logger = logger or LOGGER
    live = live_process_ids(conn, stale_seconds, now=now)
    active = conn.execute(
        """
        SELECT id, sample_id, status, owner_id
          FROM jobs
         WHERE status IN ('queued', 'running')
        """
    ).fetchall()

    live_samples: set[str] = set()
    reclaimed: list[str] = []
    for job in active:
        owner = job["owner_id"]
        if owner is not None and owner in live:
            live_samples.add(job["sample_id"])
            continue

        sample_id = job["sample_id"]
        snapshot = snapshot_for_sample(Path(packages_root), sample_id)
        if (
            snapshot is not None
            and (snapshot.manifest or {}).get("job_id") == job["id"]
        ):
            # The verified release had been swapped in just before the owner
            # died.  It is complete and must correspond to a succeeded job.
            _recover_succeeded_job(conn, job["id"], snapshot)
            logger.info(
                "任务 %s 的进程已退出，但样本包已完整发布，恢复为成功状态",
                job["id"],
            )
        else:
            interrupt_job(conn, job["id"])
            logger.info(
                "任务 %s 的负责进程 %s 已退出，标记为中断并允许重传",
                job["id"],
                owner,
            )
        _remove_staging(Path(staging_root), job["id"], logger)
        reclaimed.append(job["id"])

    # Only clean half-built releases for samples without a live owner, and only
    # old symlink temporaries, so a concurrent publisher is never disrupted.
    cleanup_incomplete(
        Path(packages_root),
        skip_samples=live_samples,
        tmp_min_age_seconds=max(60.0, float(stale_seconds)),
    )
    sweep_unknown_staging(
        conn, Path(staging_root), orphan_age_seconds, logger
    )
    conn.commit()
    return reclaimed


# ---------------------------------------------------------------------------
# Per-process lifecycle (heartbeat + periodic reaping)
# ---------------------------------------------------------------------------


class ProcessSupervisor:
    """Owns the registry row, heartbeats and runs periodic recovery."""

    def __init__(
        self,
        db_path: Path,
        *,
        packages_root: Path,
        staging_root: Path,
        stale_seconds: float,
        heartbeat_interval: float = 2.0,
        reap_interval: float = 10.0,
        orphan_age_seconds: float = 3600.0,
        process_id: str | None = None,
        connect=None,
        logger: logging.Logger | None = None,
    ):
        self.process_id = process_id or uuid.uuid4().hex
        self._db_path = Path(db_path)
        self._packages_root = Path(packages_root)
        self._staging_root = Path(staging_root)
        self._stale_seconds = float(stale_seconds)
        self._heartbeat_interval = float(heartbeat_interval)
        self._reap_interval = float(reap_interval)
        self._orphan_age_seconds = float(orphan_age_seconds)
        self._connect = connect
        self._logger = logger or LOGGER
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False

    def _new_conn(self) -> sqlite3.Connection:
        if self._connect is not None:
            return self._connect(self._db_path)
        conn = sqlite3.connect(str(self._db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def start(self) -> None:
        conn = self._new_conn()
        try:
            register_process(conn, self.process_id)
        finally:
            conn.close()
        self._thread = threading.Thread(
            target=self._run,
            name=f"process-supervisor-{self.process_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        ticks = 0
        while not self._stop.is_set():
            if self._stop.wait(self._heartbeat_interval):
                break
            ticks += 1
            try:
                conn = self._new_conn()
                try:
                    if not heartbeat(conn, self.process_id):
                        return
                    if ticks * self._heartbeat_interval >= self._reap_interval:
                        ticks = 0
                        reap_jobs(
                            conn,
                            packages_root=self._packages_root,
                            staging_root=self._staging_root,
                            stale_seconds=self._stale_seconds,
                            orphan_age_seconds=self._orphan_age_seconds,
                            logger=self._logger,
                        )
                finally:
                    conn.close()
            except sqlite3.Error:
                self._logger.warning("进程心跳/回收轮询失败", exc_info=True)

    def shutdown(self) -> None:
        """Deregister and run one final recovery pass.

        Jobs owned by this process become reclaimable immediately (the row is
        gone), while other processes keep being protected by their fresh
        heartbeats.
        """

        if self._closed:
            return
        self._closed = True
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
        try:
            conn = self._new_conn()
            try:
                deregister_process(conn, self.process_id)
                reap_jobs(
                    conn,
                    packages_root=self._packages_root,
                    staging_root=self._staging_root,
                    stale_seconds=self._stale_seconds,
                    orphan_age_seconds=self._orphan_age_seconds,
                    logger=self._logger,
                )
            finally:
                conn.close()
        except sqlite3.Error:
            self._logger.warning("进程退出回收失败", exc_info=True)
