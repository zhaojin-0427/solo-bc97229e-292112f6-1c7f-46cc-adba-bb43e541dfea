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
        while time.time() < deadline:
            response = self.client.get(f"/api/jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            job = response.get_json()
            if job["status"] in ("succeeded", "failed"):
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


if __name__ == "__main__":
    unittest.main()
