from __future__ import annotations

import gzip
import importlib
import io
import json
import os
import tempfile
import time
import unittest
import zipfile
from pathlib import Path

try:
    import flask  # noqa: F401
except ImportError:  # pragma: no cover - core tests do not need Flask
    flask = None

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@unittest.skipIf(flask is None, "Flask is not installed; skip web integration tests")
class WebIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.data_dir = self.root / "data"
        self._old_data_dir = os.environ.get("DATA_DIR")
        os.environ["DATA_DIR"] = str(self.data_dir)
        import app as app_module

        importlib.reload(app_module)
        self.app_module = app_module
        self.app = app_module.create_app(
            {
                "TESTING": True,
                "DATA_DIR": self.data_dir,
                "JOB_WORKERS": 1,
                "MAX_UPLOAD_BYTES": 64 * 1024 * 1024,
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        if self._old_data_dir is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = self._old_data_dir
        self._tmp.cleanup()

    def _upload(self, sample_id: str, example: str):
        base = EXAMPLES / example
        with open(base / "R1.fastq.gz", "rb") as r1, open(
            base / "R2.fastq.gz", "rb"
        ) as r2:
            return self.client.post(
                "/samples",
                data={
                    "sample_id": sample_id,
                    "r1": (r1, "R1.fastq.gz"),
                    "r2": (r2, "R2.fastq.gz"),
                },
                content_type="multipart/form-data",
                headers={"Accept": "application/json"},
            )

    def _wait(self, job_id: str, timeout: float = 20.0) -> dict:
        deadline = time.time() + timeout
        terminal = ("succeeded", "failed", "interrupted")
        while time.time() < deadline:
            response = self.client.get(f"/api/jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            job = response.get_json()
            if job["status"] in terminal:
                return job
            time.sleep(0.05)
        self.fail(f"job {job_id} did not finish in time")

    def test_upload_disordered_sample_and_download_package(self) -> None:
        response = self._upload("DEMO-ORDERED", "valid-disordered")
        self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
        job_id = response.get_json()["id"]
        job = self._wait(job_id)
        self.assertEqual(job["status"], "succeeded", job)
        self.assertEqual(job["r1_reads"], 4)
        self.assertEqual(job["r2_reads"], 4)
        self.assertIsNotNone(job["current_package"])

        download = self.client.get("/samples/DEMO-ORDERED/download")
        self.assertEqual(download.status_code, 200)
        payload = download.data
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            self.assertIsNone(archive.testzip())
            r1_bytes = archive.read("DEMO-ORDERED_R1.fastq.gz")
            r2_bytes = archive.read("DEMO-ORDERED_R2.fastq.gz")
        with gzip.open(io.BytesIO(r1_bytes), "rb") as gz:
            headers = [
                line.decode().split()[0]
                for line in gz
                if line.startswith(b"@")
            ]
        with gzip.open(io.BytesIO(r2_bytes), "rb") as gz:
            headers2 = [
                line.decode().split()[0]
                for line in gz
                if line.startswith(b"@")
            ]
        self.assertEqual(
            headers,
            ["@DEMO-03/1", "@DEMO-01/1", "@DEMO-04/1", "@DEMO-02/1"],
        )
        self.assertEqual(
            headers2,
            ["@DEMO-03/2", "@DEMO-01/2", "@DEMO-04/2", "@DEMO-02/2"],
        )

    def test_reupload_failure_keeps_existing_package_downloadable(self) -> None:
        first = self._upload("DEMO-KEEP", "valid-disordered")
        self.assertEqual(first.status_code, 202)
        first_job = self._wait(first.get_json()["id"])
        self.assertEqual(first_job["status"], "succeeded")
        release_id = first_job["current_package"]["release_id"]
        before = self.client.get("/samples/DEMO-KEEP/download").data

        second = self._upload("DEMO-KEEP", "tail-corrupt-gzip")
        self.assertEqual(second.status_code, 202)
        second_job = self._wait(second.get_json()["id"])
        self.assertEqual(second_job["status"], "failed", second_job)
        self.assertIn("gzip", second_job["error"])
        self.assertEqual(
            second_job["current_package"]["release_id"], release_id
        )

        after = self.client.get("/samples/DEMO-KEEP/download")
        self.assertEqual(after.status_code, 200)
        self.assertEqual(after.data, before)

    def test_invalid_sample_id_and_missing_files_are_rejected(self) -> None:
        bad = self.client.post(
            "/samples",
            data={"sample_id": "../bad"},
            headers={"Accept": "application/json"},
            content_type="multipart/form-data",
        )
        self.assertEqual(bad.status_code, 400)
        self.assertIn("样本号无效", bad.get_json()["error"])

        empty = self.client.post(
            "/samples",
            data={"sample_id": "OK"},
            headers={"Accept": "application/json"},
            content_type="multipart/form-data",
        )
        self.assertEqual(empty.status_code, 400)

    def _start_job_and_pause(self, sample_id: str, example: str) -> str:
        """Submit a job and keep the ingest worker busy until tests end."""

        import threading

        import ingest as ingest_module

        original = ingest_module.run_ingest
        resume = threading.Event()
        reached = threading.Event()

        def slow_ingest(*args, **kwargs):
            reached.set()
            resume.wait(5.0)
            return original(*args, **kwargs)

        ingest_module.run_ingest = slow_ingest

        def restore():
            resume.set()
            ingest_module.run_ingest = original

        self.addCleanup(restore)

        response = self._upload(sample_id, example)
        self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
        job_id = response.get_json()["id"]
        self.assertTrue(reached.wait(5.0))
        return job_id, resume

    def test_second_process_does_not_reclaim_live_owner_jobs(self) -> None:
        job_id, resume = self._start_job_and_pause(
            "DEMO-LIVE", "valid-disordered"
        )
        deadline = time.time() + 5.0
        status = None
        while time.time() < deadline:
            status = self.client.get(f"/api/jobs/{job_id}").get_json()
            if status["status"] == "running":
                break
            time.sleep(0.02)
        self.assertEqual(status["status"], "running")
        staging = self.data_dir / "staging" / job_id
        self.assertTrue(staging.is_dir())

        # A second service process starts against the same data directory while
        # the first process keeps heartbeating.
        second = self.app_module.create_app(
            {
                "TESTING": True,
                "DATA_DIR": self.data_dir,
                "JOB_WORKERS": 1,
                "MAX_UPLOAD_BYTES": 64 * 1024 * 1024,
                "PROCESS_STALE_SECONDS": 30,
            }
        )
        try:
            db = self.app_module.connect_main_db(
                self.data_dir / "ingest.sqlite3"
            )
            try:
                row = db.execute(
                    "SELECT status, owner_id FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()
            finally:
                db.close()
            self.assertEqual(row["status"], "running")
            self.assertIsNotNone(row["owner_id"])
            self.assertTrue(staging.is_dir())

            # The same sample still rejects duplicate uploads (409), while an
            # unrelated sample is accepted for normal validation (it fails on
            # missing files with 400, never a per-sample lock error).
            duplicate = self.client.post(
                "/samples",
                data={"sample_id": "DEMO-LIVE"},
                headers={"Accept": "application/json"},
                content_type="multipart/form-data",
            )
            self.assertEqual(duplicate.status_code, 409)
            other = second.test_client().post(
                "/samples",
                data={"sample_id": "DEMO-OTHER"},
                headers={"Accept": "application/json"},
                content_type="multipart/form-data",
            )
            self.assertEqual(other.status_code, 400)

            # Let the original job finish; it must end in success even though a
            # second process was started against the same data directory.
            resume.set()
            job = self._wait(job_id)
            self.assertEqual(job["status"], "succeeded", job)
        finally:
            second.extensions["process_supervisor"].shutdown()

    def test_interrupted_jobs_allow_reupload_and_keep_old_package(self) -> None:
        from datetime import datetime, timedelta, timezone

        import ownership

        first = self._upload("DEMO-RETRY", "valid-disordered")
        first_job = self._wait(first.get_json()["id"])
        self.assertEqual(first_job["status"], "succeeded")
        release_id = first_job["current_package"]["release_id"]
        before = self.client.get("/samples/DEMO-RETRY/download").data

        # Simulate a job whose owner process died without heartbeats.
        conn = self.app_module.connect_main_db(
            self.data_dir / "ingest.sqlite3"
        )
        try:
            dead_owner = "d" * 32
            ownership.register_process(conn, dead_owner)
            stale = (
                datetime.now(timezone.utc) - timedelta(seconds=120)
            ).isoformat(timespec="seconds")
            conn.execute(
                "UPDATE processes SET heartbeat_at = ? WHERE process_id = ?",
                (stale, dead_owner),
            )
            conn.execute(
                """
                INSERT INTO jobs(id, sample_id, status, stage, progress,
                                 created_at, owner_id)
                VALUES (?, 'DEMO-RETRY', 'running', '处理中', 40, ?, ?)
                """,
                ("e" * 32, ownership.utc_now_iso(), dead_owner),
            )
            conn.commit()
        finally:
            conn.close()

        # The periodic reaper (or next process start) marks it interrupted.
        reaper_conn = self.app_module.connect_main_db(
            self.data_dir / "ingest.sqlite3"
        )
        try:
            reclaimed = ownership.reap_jobs(
                reaper_conn,
                packages_root=self.data_dir / "packages",
                staging_root=self.data_dir / "staging",
                stale_seconds=30,
                logger=self.app.logger,
            )
        finally:
            reaper_conn.close()
        self.assertIn("e" * 32, reclaimed)
        interrupted = self.client.get("/api/jobs/" + "e" * 32).get_json()
        self.assertEqual(interrupted["status"], "interrupted")
        # The previously published package survives the interruption.
        self.assertEqual(
            interrupted["current_package"]["release_id"], release_id
        )

        # Re-upload now succeeds and becomes the current package; the old
        # release remains on disk for historical downloads.
        retry = self._upload("DEMO-RETRY", "valid-disordered")
        retry_job = self._wait(retry.get_json()["id"])
        self.assertEqual(retry_job["status"], "succeeded")
        self.assertNotEqual(
            retry_job["current_package"]["release_id"], release_id
        )
        self.assertEqual(
            self.client.get("/samples/DEMO-RETRY/download").status_code, 200
        )
        first_release = (
            self.data_dir / "packages" / "DEMO-RETRY" / "releases" / release_id
        )
        self.assertTrue((first_release / "package.zip").is_file())
        self.assertTrue(before)


if __name__ == "__main__":
    unittest.main()
