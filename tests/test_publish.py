from __future__ import annotations

import gzip
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from publish import (
    cleanup_incomplete,
    create_release,
    prune_releases,
    snapshot_for_sample,
)


def _write_gz(path: Path, text: str) -> None:
    with open(path, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            gz.write(text.encode())


def _make_work(work: Path, label: str) -> None:
    work.mkdir(parents=True, exist_ok=True)
    _write_gz(work / "R1.fastq.gz", f"@p1/1 {label}\nAC\n+\nII\n")
    _write_gz(work / "R2.fastq.gz", f"@p1/2 {label}\nGT\n+\nII\n")


class PublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.packages = self.root / "packages"
        self.work = self.root / "work"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _publish(self, job_char: str, label: str = "one"):
        _make_work(self.work, label)
        return create_release(
            work_dir=self.work,
            packages_root=self.packages,
            sample_id="SAMPLE-1",
            job_id=job_char * 32,
            read_count=1,
            r1_sha256="r1hash",
            r2_sha256="r2hash",
            keep_releases=2,
        )

    def test_release_contains_two_gz_files_manifest_and_verified_zip(self) -> None:
        snapshot = self._publish("a")
        self.assertTrue((snapshot.directory / "R1.fastq.gz").is_file())
        self.assertTrue((snapshot.directory / "R2.fastq.gz").is_file())
        self.assertTrue(snapshot.manifest_path.is_file())
        self.assertTrue((snapshot.directory / ".complete").is_file())
        with zipfile.ZipFile(snapshot.package_zip) as archive:
            self.assertIsNone(archive.testzip())
            self.assertEqual(
                set(archive.namelist()),
                {
                    "SAMPLE-1_R1.fastq.gz",
                    "SAMPLE-1_R2.fastq.gz",
                    "manifest.json",
                },
            )
            manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["read_count"], 1)
            self.assertEqual(manifest["job_id"], "a" * 32)
            self.assertEqual(manifest["ordering"], "R1 original input order")

    def test_replacement_is_atomic_and_keeps_old_release(self) -> None:
        first = self._publish("a", "first")
        old_zip = first.package_zip
        old_release_dir = first.directory
        second = self._publish("b", "second")
        self.assertNotEqual(first.release_id, second.release_id)
        self.assertEqual(
            os.readlink(self.packages / "SAMPLE-1" / "current"),
            os.path.join("releases", second.release_id),
        )
        self.assertTrue(old_release_dir.is_dir())
        self.assertTrue(old_zip.is_file())
        self.assertEqual(snapshot_for_sample(self.packages, "SAMPLE-1").release_id, second.release_id)

    def test_failed_build_does_not_touch_current(self) -> None:
        published = self._publish("a")
        self.work.mkdir(parents=True, exist_ok=True)
        _write_gz(self.work / "R1.fastq.gz", "@p1/1\nAC\n+\nII\n")
        (self.work / "R2.fastq.gz").unlink(missing_ok=True)
        with self.assertRaisesRegex(Exception, "不完整"):
            create_release(
                work_dir=self.work,
                packages_root=self.packages,
                sample_id="SAMPLE-1",
                job_id="b" * 32,
                read_count=1,
                r1_sha256="r1hash",
                r2_sha256="r2hash",
                keep_releases=2,
            )
        current = snapshot_for_sample(self.packages, "SAMPLE-1")
        self.assertIsNotNone(current)
        self.assertEqual(current.release_id, published.release_id)
        self.assertEqual(current.directory, published.directory)

    def test_prune_keeps_current_and_previous_release(self) -> None:
        first = self._publish("a", "first")
        second = self._publish("b", "second")
        third = self._publish("c", "third")
        prune_releases(self.packages / "SAMPLE-1", 2)
        current = snapshot_for_sample(self.packages, "SAMPLE-1")
        self.assertEqual(current.release_id, third.release_id)
        self.assertTrue(second.directory.is_dir())
        self.assertFalse(first.directory.is_dir())

    def test_cleanup_removes_incomplete_release_but_keeps_current(self) -> None:
        published = self._publish("a")
        incomplete = published.directory.parent / "INCOMPLETE"
        incomplete.mkdir()
        (incomplete / "R1.fastq.gz").write_bytes(b"partial")
        cleanup_incomplete(self.packages)
        self.assertTrue(published.directory.is_dir())
        self.assertFalse(incomplete.exists())


if __name__ == "__main__":
    unittest.main()
