"""Atomic sample-package publication.

A release is fully built in its own directory and only made visible by an
atomic symlink replacement.  The previously published release directory is
kept in place, so an in-flight or repeat download of the old package continues
to work while a replacement is being prepared.
"""

from __future__ import annotations

import json
import os
import shutil
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

MANIFEST_NAME = "manifest.json"
ZIP_NAME = "package.zip"
COMPLETE_MARKER = ".complete"
R1_NAME = "R1.fastq.gz"
R2_NAME = "R2.fastq.gz"


class PublishError(Exception):
    """Package publication failed; the current package must remain untouched."""


class PackageSnapshot(NamedTuple):
    sample_id: str
    release_id: str
    directory: Path
    package_zip: Path
    r1_path: Path
    r2_path: Path
    manifest_path: Path
    manifest: dict


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_release_id() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_DIRECTORY)
    except (AttributeError, OSError):
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_file(path: Path) -> None:
    with open(path, "rb") as fh:
        os.fsync(fh.fileno())


def _zip_entry_names(sample_id: str) -> tuple[str, str, str]:
    return (
        f"{sample_id}_R1.fastq.gz",
        f"{sample_id}_R2.fastq.gz",
        MANIFEST_NAME,
    )


def build_package_zip(
    release_dir: Path, sample_id: str, manifest_path: Path
) -> Path:
    r1_entry, r2_entry, manifest_entry = _zip_entry_names(sample_id)
    r1_path = release_dir / R1_NAME
    r2_path = release_dir / R2_NAME
    zip_tmp = release_dir / (ZIP_NAME + ".tmp")
    zip_path = release_dir / ZIP_NAME
    try:
        with zipfile.ZipFile(zip_tmp, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.write(r1_path, arcname=r1_entry)
            archive.write(r2_path, arcname=r2_entry)
            archive.write(manifest_path, arcname=manifest_entry)
        _fsync_file(zip_tmp)
        os.replace(zip_tmp, zip_path)
        _fsync_dir(release_dir)
        _verify_package_zip(zip_path, sample_id)
    except Exception:
        try:
            zip_tmp.unlink()
        except FileNotFoundError:
            pass
        raise
    return zip_path


def _verify_package_zip(zip_path: Path, sample_id: str) -> None:
    names = _zip_entry_names(sample_id)
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            actual = set(archive.namelist())
            missing = set(names) - actual
            if missing:
                raise PublishError(
                    "样本包 ZIP 缺少条目：" + "、".join(sorted(missing))
                )
            bad = archive.testzip()
            if bad is not None:
                raise PublishError(f"样本包 ZIP CRC 校验失败：{bad}")
    except zipfile.BadZipFile as exc:
        raise PublishError(f"样本包 ZIP 无效：{exc}") from exc


def _write_manifest(release_dir: Path, manifest: dict) -> Path:
    manifest_path = release_dir / MANIFEST_NAME
    tmp_path = release_dir / (MANIFEST_NAME + ".tmp")
    payload = json.dumps(
        manifest, ensure_ascii=False, indent=2, sort_keys=True
    ).encode("utf-8")
    with open(tmp_path, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, manifest_path)
    _fsync_file(manifest_path)
    return manifest_path


def _atomic_set_current(
    sample_dir: Path,
    release_id: str,
    expected_current: str | None = None,
) -> None:
    """Atomically point ``current`` at ``release_id``.

    ``expected_current`` is a fencing token: the symlink target that the caller
    observed before building the release.  If another process has meanwhile
    published a newer package, the replacement is refused instead of silently
    overwriting it.  ``None`` means the caller expects no current package yet.
    """

    current = sample_dir / "current"
    observed = _current_target(sample_dir)
    if expected_current is not None and observed != expected_current:
        raise PublishError(
            "发布被拒绝：样本包已被更新的任务发布，本次旧执行不得覆盖新包"
        )
    tmp_link = sample_dir / f".current-{uuid.uuid4().hex}.tmp"
    os.symlink(os.path.join("releases", release_id), tmp_link, target_is_directory=True)
    try:
        # Re-check with the replacement staged so the window between the check
        # and the atomic rename is covered as well.
        observed = _current_target(sample_dir)
        if expected_current is not None and observed != expected_current:
            raise PublishError(
                "发布被拒绝：样本包已被更新的任务发布，本次旧执行不得覆盖新包"
            )
        os.replace(tmp_link, current)
        _fsync_dir(sample_dir)
    finally:
        try:
            tmp_link.unlink()
        except FileNotFoundError:
            pass


def _current_target(sample_dir: Path) -> str | None:
    current = sample_dir / "current"
    if not os.path.lexists(current):
        return None
    try:
        target = os.readlink(current)
    except OSError:
        return None
    return target


def prune_releases(sample_dir: Path, keep: int) -> None:
    """Keep the current release plus the newest completed historical releases."""

    keep = max(2, int(keep))
    current_target = _current_target(sample_dir)
    releases_dir = sample_dir / "releases"
    if not releases_dir.is_dir():
        return

    current_name = None
    if current_target:
        current_name = Path(current_target).name

    releases = []
    for item in releases_dir.iterdir():
        if not item.is_dir():
            continue
        if not (item / COMPLETE_MARKER).exists():
            continue
        if current_name and item.name == current_name:
            releases.append((1_000_000_000_000.0, item))
        else:
            try:
                mtime = item.stat().st_mtime
            except OSError:
                mtime = 0.0
            releases.append((mtime, item))

    releases.sort(key=lambda pair: (pair[0], pair[1].name), reverse=True)
    for _, item in releases[keep:]:
        if current_name and item.name == current_name:
            continue
        try:
            shutil.rmtree(item)
        except OSError:
            pass


def create_release(
    *,
    work_dir: Path,
    packages_root: Path,
    sample_id: str,
    job_id: str,
    read_count: int,
    r1_sha256: str,
    r2_sha256: str,
    keep_releases: int,
    expected_current: str | None = None,
) -> PackageSnapshot:
    """Move verified outputs into a release and atomically publish it.

    ``work_dir`` must contain ``R1.fastq.gz`` and ``R2.fastq.gz`` which have
    already passed decompression/structure verification.

    ``expected_current`` is the symlink target (e.g. ``releases/<id>``) the
    caller observed before starting; the publish is fenced on it so a stale
    execution can never overwrite a package a newer task published later.
    Pass ``None`` when no package existed for the sample.
    """

    source_r1 = work_dir / R1_NAME
    source_r2 = work_dir / R2_NAME
    if not source_r1.is_file() or not source_r2.is_file():
        raise PublishError("待发布的 R1/R2 文件不完整")

    release_id = _new_release_id()
    sample_dir = packages_root / sample_id
    releases_dir = sample_dir / "releases"
    release_dir = releases_dir / release_id
    completed = False

    try:
        releases_dir.mkdir(parents=True, exist_ok=True)
        release_dir.mkdir(parents=False, exist_ok=False)
        os.replace(source_r1, release_dir / R1_NAME)
        os.replace(source_r2, release_dir / R2_NAME)
        _fsync_file(release_dir / R1_NAME)
        _fsync_file(release_dir / R2_NAME)

        r1_size = (release_dir / R1_NAME).stat().st_size
        r2_size = (release_dir / R2_NAME).stat().st_size
        r1_entry, r2_entry, manifest_entry = _zip_entry_names(sample_id)
        manifest = {
            "format": "paired-fastq-sample/v1",
            "sample_id": sample_id,
            "job_id": job_id,
            "release_id": release_id,
            "created_at": utc_now_iso(),
            "ordering": "R1 original input order",
            "read_count": read_count,
            "files": {
                "R1": {
                    "file": R1_NAME,
                    "sha256": r1_sha256,
                    "size": r1_size,
                },
                "R2": {
                    "file": R2_NAME,
                    "sha256": r2_sha256,
                    "size": r2_size,
                },
            },
            "archive": {
                "file": ZIP_NAME,
                "entries": [r1_entry, r2_entry, manifest_entry],
            },
        }
        manifest_path = _write_manifest(release_dir, manifest)
        _fsync_dir(release_dir)

        build_package_zip(release_dir, sample_id, manifest_path)

        marker_tmp = release_dir / (COMPLETE_MARKER + ".tmp")
        with open(marker_tmp, "wb") as fh:
            fh.write(b"ok\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(marker_tmp, release_dir / COMPLETE_MARKER)
        _fsync_dir(release_dir)

        # Last step: the complete release becomes visible in one filesystem
        # operation.  Until this succeeds, 'current' still points at the old,
        # fully intact package.  Do not delete the release from a finally block
        # once this phase starts: a swap may already have succeeded.
        completed = True
        _atomic_set_current(sample_dir, release_id, expected_current)
        prune_releases(sample_dir, keep_releases)
        return read_snapshot(sample_dir, sample_id)
    finally:
        if not completed:
            try:
                shutil.rmtree(release_dir)
            except OSError:
                pass


def read_snapshot(sample_dir: Path, expected_sample_id: str | None = None) -> PackageSnapshot | None:
    current = sample_dir / "current"
    if not os.path.lexists(current):
        return None
    directory = Path(os.path.realpath(current))
    manifest_path = directory / MANIFEST_NAME
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, ValueError):
        return None
    sample_id = manifest.get("sample_id")
    release_id = manifest.get("release_id")
    if not isinstance(sample_id, str) or not isinstance(release_id, str):
        return None
    if expected_sample_id is not None and sample_id != expected_sample_id:
        return None
    package_zip = directory / ZIP_NAME
    if (
        not package_zip.is_file()
        or not (directory / R1_NAME).is_file()
        or not (directory / R2_NAME).is_file()
        or not (directory / COMPLETE_MARKER).is_file()
    ):
        return None
    return PackageSnapshot(
        sample_id=sample_id,
        release_id=release_id,
        directory=directory,
        package_zip=package_zip,
        r1_path=directory / R1_NAME,
        r2_path=directory / R2_NAME,
        manifest_path=manifest_path,
        manifest=manifest,
    )


def snapshot_for_sample(packages_root: Path, sample_id: str) -> PackageSnapshot | None:
    return read_snapshot(packages_root / sample_id, sample_id)


def cleanup_incomplete(
    packages_root: Path,
    skip_samples: set[str] | None = None,
    tmp_min_age_seconds: float = 0.0,
) -> None:
    """Remove half-built releases and stale symlink temporaries.

    Samples listed in ``skip_samples`` still have a live owner process: their
    directories are left entirely alone.  Symlink temporaries younger than
    ``tmp_min_age_seconds`` are kept so a concurrent publisher is never disturbed
    mid-swap.
    """

    skip_samples = skip_samples or set()
    now = time.time()
    if not packages_root.is_dir():
        return
    for sample_dir in packages_root.iterdir():
        if not sample_dir.is_dir():
            continue
        if sample_dir.name in skip_samples:
            continue
        for tmp in sample_dir.glob(".current-*.tmp"):
            try:
                if now - tmp.stat().st_mtime < tmp_min_age_seconds:
                    continue
            except OSError:
                pass
            try:
                tmp.unlink()
            except OSError:
                pass
        current = sample_dir / "current"
        current_target = _current_target(sample_dir)
        current_name = Path(current_target).name if current_target else None
        releases_dir = sample_dir / "releases"
        if not releases_dir.is_dir():
            continue
        for release in releases_dir.iterdir():
            if not release.is_dir():
                continue
            if current_name and release.name == current_name:
                continue
            if not (release / COMPLETE_MARKER).exists():
                try:
                    shutil.rmtree(release)
                except OSError:
                    pass


def iter_snapshots(packages_root: Path):
    if not packages_root.is_dir():
        return
    for sample_dir in sorted(packages_root.iterdir(), key=lambda p: p.name):
        if not sample_dir.is_dir():
            continue
        snapshot = read_snapshot(sample_dir)
        if snapshot is not None:
            yield snapshot
