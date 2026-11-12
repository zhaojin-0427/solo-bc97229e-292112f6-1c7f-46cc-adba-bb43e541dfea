"""Flask sample-ingest service.

The web process streams uploads to disk, then hands each job to a background
worker.  Pairing and publication are implemented in ingest.py/publish.py.
"""

from __future__ import annotations

import atexit
import logging
import os
import re
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

import ownership
from ingest import IngestError, run_ingest
from publish import (
    PublishError,
    _current_target,
    create_release,
    iter_snapshots,
    snapshot_for_sample,
)

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")
UPLOAD_CHUNK = 1024 * 1024
STATUS_UPDATE_INTERVAL = 0.30

STATUS_LABELS = {
    "queued": "排队中",
    "running": "处理中",
    "succeeded": "已完成",
    "failed": "失败",
    "interrupted": "已中断",
}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _readable_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _tail(text: str, limit: int = 4000) -> str:
    text = str(text).strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _sample_id_from_form() -> str:
    return request.form.get("sample_id", "").strip()


def _wants_json() -> bool:
    accept = request.headers.get("Accept", "")
    return (
        "application/json" in accept
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )


def _save_upload(storage, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with open(destination, "wb") as out:
        while True:
            chunk = storage.stream.read(UPLOAD_CHUNK)
            if not chunk:
                break
            out.write(chunk)
            size += len(chunk)
        out.flush()
        os.fsync(out.fileno())
    return size


def _configure_main_connection(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")


def connect_main_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    _configure_main_connection(conn)
    return conn


def _init_main_db(path: Path) -> None:
    conn = connect_main_db(path)
    try:
        ownership.init_schema(conn)
    finally:
        conn.close()


def _utc_now() -> str:
    from publish import utc_now_iso

    return utc_now_iso()


def _rebuild_package_catalog(conn: sqlite3.Connection, packages_root: Path) -> list:
    """Re-sync the packages table from the packages on disk.

    Published packages live entirely on disk under ``current`` symlinks, so the
    catalog is always rebuildable and an in-flight result is never republished.
    """

    snapshots = list(iter_snapshots(packages_root))
    seen = set()
    for snapshot in snapshots:
        seen.add(snapshot.sample_id)
        ownership.upsert_package(conn, snapshot)
    if seen:
        placeholders = ",".join("?" for _ in seen)
        conn.execute(
            f"DELETE FROM packages WHERE sample_id NOT IN ({placeholders})",
            tuple(seen),
        )
    else:
        conn.execute("DELETE FROM packages")
    conn.commit()
    return snapshots


def _reconcile_startup(app: Flask) -> None:
    """Rebuild the package catalog and reclaim jobs of dead owner processes.

    Jobs whose owner still holds a fresh heartbeat (for example when a second
    process is started against the same data directory during an upgrade) are
    never touched: their staging files stay on disk, their status stays
    queued/running and the sample keeps rejecting duplicate uploads.
    """

    packages_root: Path = app.config["PACKAGES_DIR"]
    staging_root: Path = app.config["STAGING_DIR"]
    conn = connect_main_db(app.config["DB_PATH"])
    try:
        _rebuild_package_catalog(conn, packages_root)
        ownership.reap_jobs(
            conn,
            packages_root=packages_root,
            staging_root=staging_root,
            stale_seconds=float(app.config["PROCESS_STALE_SECONDS"]),
            orphan_age_seconds=float(app.config["STAGING_ORPHAN_SECONDS"]),
            logger=app.logger,
        )
    finally:
        conn.close()


def _progress_writer(conn: sqlite3.Connection, job_id: str, owner_id: str):
    last = {"time": 0.0, "stage": None, "percent": None}

    def update(stage: str, percent: int, r1_reads: int, r2_reads: int) -> None:
        now = time.monotonic()
        if (
            stage != last["stage"]
            or percent >= 100
            or (percent != last["percent"] and now - last["time"] >= STATUS_UPDATE_INTERVAL)
        ):
            last.update(time=now, stage=stage, percent=percent)
            try:
                if not ownership.write_progress(
                    conn, job_id, owner_id, stage, percent, r1_reads, r2_reads
                ):
                    # Another process reclaimed this job after the owner died.
                    raise ownership.JobOwnershipLost(job_id)
            except sqlite3.Error:
                # A transient status write must not abort a valid ingest.
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                app_logger().warning("写入任务进度失败", exc_info=True)

    return update


def app_logger():
    return logging.getLogger("sample_ingest")


def _run_job(config: dict, job_id: str) -> None:
    db_path = Path(config["DB_PATH"])
    staging_root = Path(config["STAGING_DIR"])
    packages_root = Path(config["PACKAGES_DIR"])
    keep_releases = int(config["KEEP_RELEASES"])
    owner_id = config["PROCESS_ID"]
    conn = connect_main_db(db_path)
    work_dir = staging_root / job_id
    logger = app_logger()
    try:
        job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if job is None:
            return
        if job["status"] != "queued" or job["owner_id"] != owner_id:
            # Another process owns the job, or it was already reclaimed.
            return
        started = _utc_now()
        if not ownership.claim_running(conn, job_id, owner_id, started):
            logger.info("任务 %s 已不归本进程所有，跳过执行", job_id)
            return

        update_progress = _progress_writer(conn, job_id, owner_id)
        logger.info("任务 %s 开始：样本 %s", job_id, job["sample_id"])
        input_dir = work_dir / "input"
        output_dir = work_dir / "output"
        result = run_ingest(
            input_dir / "R1.fastq.gz",
            input_dir / "R2.fastq.gz",
            output_dir,
            progress=update_progress,
        )

        # Publication fencing: remember the package currently visible for the
        # sample before swapping.  If a newer task published in the meantime
        # (possible after this process was reclaimed and the sample retried),
        # create_release refuses to overwrite it.
        sample_dir = packages_root / job["sample_id"]
        expected_current = _current_target(sample_dir)
        if not ownership.write_progress(
            conn, job_id, owner_id, "生成并原子发布样本包", 99,
            result["read_count"], result["read_count"],
        ):
            raise ownership.JobOwnershipLost(job_id)

        snapshot = create_release(
            work_dir=output_dir,
            packages_root=packages_root,
            sample_id=job["sample_id"],
            job_id=job_id,
            read_count=result["read_count"],
            r1_sha256=result["r1_sha256"],
            r2_sha256=result["r2_sha256"],
            keep_releases=keep_releases,
            expected_current=expected_current,
        )
        if snapshot is None:
            raise PublishError("发布完成但无法读取新样本包清单")

        finished = _utc_now()
        for attempt in range(6):
            try:
                if ownership.complete_job(
                    conn, job_id, owner_id, snapshot,
                    result["read_count"], finished,
                ):
                    break
                # The job was reclaimed while this worker was busy.  The swap
                # itself was fenced, so the on-disk package cannot have
                # regressed; the reaper reconciles the row from disk.
                logger.warning(
                    "任务 %s 发布完成但已被其他进程回收，跳过终态写入", job_id
                )
                return
            except sqlite3.OperationalError:
                if attempt == 5:
                    logger.error("任务 %s 发布成功但写数据库失败", job_id, exc_info=True)
                    # The package is already live.  Recovery will reconcile
                    # the status once this process stops heartbeating.
                    return
                time.sleep(0.2)
        logger.info(
            "任务 %s 完成：样本 %s，%s 条记录，release %s",
            job_id,
            job["sample_id"],
            result["read_count"],
            snapshot.release_id,
        )
    except ownership.JobOwnershipLost:
        logger.warning(
            "任务 %s 的归属已转移到其他进程，本进程停止后续写入", job_id
        )
    except (IngestError, PublishError) as exc:
        logger.warning("任务 %s 失败：%s", job_id, exc)
        try:
            if not ownership.fail_job(conn, job_id, owner_id, _tail(str(exc))):
                logger.warning(
                    "任务 %s 已被回收，失败状态由恢复流程处理", job_id
                )
        except sqlite3.Error:
            logger.error("记录任务失败状态时数据库出错", exc_info=True)
    except Exception as exc:  # pragma: no cover - defensive path
        logger.error("任务 %s 发生未预期错误", job_id, exc_info=True)
        try:
            if not ownership.fail_job(
                conn,
                job_id,
                owner_id,
                _tail(f"处理失败（{type(exc).__name__}）：{exc}"),
            ):
                logger.warning(
                    "任务 %s 已被回收，失败状态由恢复流程处理", job_id
                )
        except sqlite3.Error:
            logger.error("记录任务失败状态时数据库出错", exc_info=True)
    finally:
        try:
            conn.close()
        finally:
            # Only the owner may remove its own work directory.  A reclaimed job
            # keeps its staging until the reaper removes it (or never, if the
            # reclaiming process is still using this directory's sibling data).
            try:
                owner_now = None
                check = connect_main_db(db_path)
                try:
                    row = check.execute(
                        "SELECT owner_id FROM jobs WHERE id = ?", (job_id,)
                    ).fetchone()
                    if row is not None:
                        owner_now = row["owner_id"]
                finally:
                    check.close()
                if owner_now == owner_id or owner_now is None:
                    shutil.rmtree(work_dir)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning("无法清理任务工作目录：%s", work_dir, exc_info=True)


def _job_payload(app: Flask, row: sqlite3.Row) -> dict:
    snapshot = snapshot_for_sample(app.config["PACKAGES_DIR"], row["sample_id"])
    payload = {
        "id": row["id"],
        "sample_id": row["sample_id"],
        "status": row["status"],
        "status_label": STATUS_LABELS.get(row["status"], row["status"]),
        "stage": row["stage"],
        "progress": row["progress"],
        "r1_reads": row["r1_reads"],
        "r2_reads": row["r2_reads"],
        "error": row["error"],
        "release_id": row["release_id"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "current_package": None,
    }
    if snapshot is not None:
        manifest = snapshot.manifest
        payload["current_package"] = {
            "sample_id": snapshot.sample_id,
            "release_id": snapshot.release_id,
            "read_count": _readable_int(manifest.get("read_count")),
            "created_at": manifest.get("created_at"),
            "download_url": url_for(
                "download_package", sample_id=snapshot.sample_id
            ),
            "r1_url": url_for("download_r1", sample_id=snapshot.sample_id),
            "r2_url": url_for("download_r2", sample_id=snapshot.sample_id),
        }
    return payload


def _package_cards(app: Flask) -> list[dict]:
    cards = []
    for snapshot in iter_snapshots(app.config["PACKAGES_DIR"]):
        manifest = snapshot.manifest
        files = manifest.get("files") or {}
        cards.append(
            {
                "sample_id": snapshot.sample_id,
                "release_id": snapshot.release_id,
                "read_count": _readable_int(manifest.get("read_count")),
                "created_at": manifest.get("created_at"),
                "r1_size": _readable_int((files.get("R1") or {}).get("size")),
                "r2_size": _readable_int((files.get("R2") or {}).get("size")),
                "download_url": url_for(
                    "download_package", sample_id=snapshot.sample_id
                ),
                "r1_url": url_for("download_r1", sample_id=snapshot.sample_id),
                "r2_url": url_for("download_r2", sample_id=snapshot.sample_id),
                "manifest_url": url_for(
                    "download_manifest", sample_id=snapshot.sample_id
                ),
            }
        )
    return cards


def _http_error(app: Flask, message: str, status: int):
    if _wants_json():
        return jsonify({"error": message}), status
    return (
        render_template(
            "index.html",
            error=message,
            packages=_package_cards(app),
            max_upload=app.config["MAX_UPLOAD_BYTES"],
        ),
        status,
    )


def _valid_sample_id(sample_id: str) -> bool:
    return bool(SAMPLE_ID_RE.fullmatch(sample_id or ""))


def create_app(test_config: dict | None = None) -> Flask:
    data_dir = Path(
        os.environ.get("DATA_DIR", str(BASE_DIR / "data"))
    ).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    max_upload = _env_int("MAX_UPLOAD_BYTES", 16 * 1024 * 1024 * 1024)

    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.update(
        DATA_DIR=data_dir,
        DB_PATH=data_dir / "ingest.sqlite3",
        STAGING_DIR=data_dir / "staging",
        PACKAGES_DIR=data_dir / "packages",
        MAX_UPLOAD_BYTES=max_upload,
        MAX_CONTENT_LENGTH=max_upload if max_upload > 0 else None,
        JOB_WORKERS=max(1, _env_int("JOB_WORKERS", 1)),
        KEEP_RELEASES=max(2, _env_int("KEEP_RELEASES", 2)),
        PROCESS_STALE_SECONDS=max(
            5, _env_int("PROCESS_STALE_SECONDS", 30)
        ),
        HEARTBEAT_INTERVAL=max(
            0.5, float(os.environ.get("HEARTBEAT_INTERVAL") or 2.0)
        ),
        REAP_INTERVAL=max(
            1.0, float(os.environ.get("REAP_INTERVAL") or 10.0)
        ),
        STAGING_ORPHAN_SECONDS=max(
            60, _env_int("STAGING_ORPHAN_SECONDS", 3600)
        ),
        PROCESS_ID=uuid.uuid4().hex,
    )
    if test_config:
        app.config.update(test_config)
        if "DATA_DIR" in test_config:
            data_dir = Path(app.config["DATA_DIR"]).resolve()
            data_dir.mkdir(parents=True, exist_ok=True)
            app.config["DATA_DIR"] = data_dir
            if "DB_PATH" not in test_config:
                app.config["DB_PATH"] = data_dir / "ingest.sqlite3"
            if "STAGING_DIR" not in test_config:
                app.config["STAGING_DIR"] = data_dir / "staging"
            if "PACKAGES_DIR" not in test_config:
                app.config["PACKAGES_DIR"] = data_dir / "packages"
        if "MAX_UPLOAD_BYTES" in test_config:
            app.config["MAX_CONTENT_LENGTH"] = (
                app.config["MAX_UPLOAD_BYTES"]
                if app.config["MAX_UPLOAD_BYTES"] > 0
                else None
            )

    for key in ("STAGING_DIR", "PACKAGES_DIR"):
        Path(app.config[key]).mkdir(parents=True, exist_ok=True)
    Path(app.config["DB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    _init_main_db(Path(app.config["DB_PATH"]))
    _reconcile_startup(app)

    # Register this process and keep its heartbeat fresh; the background
    # reaper only reclaims jobs whose owner stopped heartbeating.
    supervisor = ownership.ProcessSupervisor(
        Path(app.config["DB_PATH"]),
        packages_root=Path(app.config["PACKAGES_DIR"]),
        staging_root=Path(app.config["STAGING_DIR"]),
        stale_seconds=float(app.config["PROCESS_STALE_SECONDS"]),
        heartbeat_interval=float(app.config["HEARTBEAT_INTERVAL"]),
        reap_interval=float(app.config["REAP_INTERVAL"]),
        orphan_age_seconds=float(app.config["STAGING_ORPHAN_SECONDS"]),
        process_id=app.config["PROCESS_ID"],
        connect=lambda path: connect_main_db(Path(path)),
        logger=app.logger,
    )
    supervisor.start()
    app.extensions["process_supervisor"] = supervisor
    atexit.register(supervisor.shutdown)

    executor = ThreadPoolExecutor(
        max_workers=int(app.config["JOB_WORKERS"]),
        thread_name_prefix="sample-ingest",
    )
    app.extensions["job_executor"] = executor

    def db() -> sqlite3.Connection:
        if "db" not in g:
            g.db = connect_main_db(Path(app.config["DB_PATH"]))
        return g.db

    @app.teardown_appcontext
    def close_db(_exc):
        conn = g.pop("db", None)
        if conn is not None:
            conn.close()

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            error=None,
            packages=_package_cards(app),
            max_upload=app.config["MAX_UPLOAD_BYTES"],
        )

    @app.post("/samples")
    def upload_sample():
        sample_id = _sample_id_from_form()
        if not _valid_sample_id(sample_id):
            return _http_error(
                app,
                "样本号无效：需以字母或数字开头，仅允许字母、数字、点、下划线和连字符，最长 128 字符。",
                400,
            )
        r1_file = request.files.get("r1")
        r2_file = request.files.get("r2")
        if r1_file is None or r2_file is None:
            return _http_error(app, "请同时选择 R1 和 R2 两个 gzip 文件。", 400)
        if not r1_file.filename or not r2_file.filename:
            return _http_error(app, "R1 和 R2 文件名不能为空。", 400)
        if r1_file.stream is None or r2_file.stream is None:
            return _http_error(app, "上传文件无法读取。", 400)

        active = db().execute(
            """
            SELECT id FROM jobs
             WHERE sample_id = ? AND status IN ('queued', 'running')
             LIMIT 1
            """,
            (sample_id,),
        ).fetchone()
        if active is not None:
            return _http_error(
                app,
                f"样本 {sample_id} 正在处理中，请等待本次任务结束后再重新上传。",
                409,
            )

        job_id = uuid.uuid4().hex
        work_dir = Path(app.config["STAGING_DIR"]) / job_id
        input_dir = work_dir / "input"
        try:
            input_dir.mkdir(parents=True, exist_ok=False)
            _save_upload(r1_file, input_dir / "R1.fastq.gz")
            _save_upload(r2_file, input_dir / "R2.fastq.gz")
        except Exception as exc:
            try:
                shutil.rmtree(work_dir)
            except OSError:
                pass
            app.logger.error("保存上传文件失败", exc_info=True)
            return _http_error(app, f"保存上传文件失败：{exc}", 500)

        conn = db()
        try:
            conn.execute(
                """
                INSERT INTO jobs(id, sample_id, status, stage, progress,
                                 created_at, owner_id)
                VALUES (?, ?, 'queued', '排队中', 0, ?, ?)
                """,
                (job_id, sample_id, _utc_now(), app.config["PROCESS_ID"]),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            try:
                shutil.rmtree(work_dir)
            except OSError:
                pass
            return _http_error(
                app,
                f"样本 {sample_id} 正在处理中，请稍后再试。",
                409,
            )
        except sqlite3.Error as exc:
            conn.rollback()
            try:
                shutil.rmtree(work_dir)
            except OSError:
                pass
            app.logger.error("创建任务失败", exc_info=True)
            return _http_error(app, f"创建任务失败：{exc}", 500)

        try:
            executor.submit(_run_job, dict(app.config), job_id)
        except Exception as exc:
            app.logger.error("启动后台任务失败", exc_info=True)
            try:
                ownership.fail_job(
                    conn, job_id, app.config["PROCESS_ID"],
                    _tail(f"无法启动后台任务：{exc}"),
                )
            except sqlite3.Error:
                pass
            try:
                shutil.rmtree(work_dir)
            except OSError:
                pass
            return _http_error(app, "无法启动后台处理任务。", 500)

        if _wants_json():
            return (
                jsonify(
                    {
                        "id": job_id,
                        "sample_id": sample_id,
                        "status_url": url_for("job_api", job_id=job_id),
                        "job_url": url_for("job_page", job_id=job_id),
                    }
                ),
                202,
            )
        return redirect(url_for("job_page", job_id=job_id), code=303)

    @app.get("/jobs/<job_id>")
    def job_page(job_id: str):
        if not JOB_ID_RE.fullmatch(job_id):
            abort(404)
        row = db().execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            abort(404)
        return render_template(
            "job.html",
            job=_job_payload(app, row),
        )

    @app.get("/api/jobs/<job_id>")
    def job_api(job_id: str):
        if not JOB_ID_RE.fullmatch(job_id):
            return jsonify({"error": "任务不存在"}), 404
        row = db().execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return jsonify({"error": "任务不存在"}), 404
        return jsonify(_job_payload(app, row))

    def _download_snapshot(sample_id: str):
        if not _valid_sample_id(sample_id):
            abort(404)
        snapshot = snapshot_for_sample(Path(app.config["PACKAGES_DIR"]), sample_id)
        if snapshot is None:
            abort(404)
        return snapshot

    @app.get("/samples/<sample_id>/download")
    def download_package(sample_id: str):
        snapshot = _download_snapshot(sample_id)
        return send_file(
            snapshot.package_zip,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"{sample_id}_package.zip",
            conditional=True,
        )

    @app.get("/samples/<sample_id>/download/r1")
    def download_r1(sample_id: str):
        snapshot = _download_snapshot(sample_id)
        return send_file(
            snapshot.r1_path,
            mimetype="application/gzip",
            as_attachment=True,
            download_name=f"{sample_id}_R1.fastq.gz",
            conditional=True,
        )

    @app.get("/samples/<sample_id>/download/r2")
    def download_r2(sample_id: str):
        snapshot = _download_snapshot(sample_id)
        return send_file(
            snapshot.r2_path,
            mimetype="application/gzip",
            as_attachment=True,
            download_name=f"{sample_id}_R2.fastq.gz",
            conditional=True,
        )

    @app.get("/samples/<sample_id>/manifest")
    def download_manifest(sample_id: str):
        snapshot = _download_snapshot(sample_id)
        return send_file(
            snapshot.manifest_path,
            mimetype="application/json",
            as_attachment=True,
            download_name=f"{sample_id}_manifest.json",
            conditional=True,
        )

    @app.errorhandler(413)
    def payload_too_large(_exc):
        if app.config["MAX_UPLOAD_BYTES"] > 0:
            message = (
                "上传内容过大：限制为 "
                f"{app.config['MAX_UPLOAD_BYTES']} 字节（"
                "可通过 MAX_UPLOAD_BYTES 调整）。"
            )
        else:
            message = "上传内容过大。"
        if _wants_json():
            return jsonify({"error": message}), 413
        return (
            render_template(
                "index.html",
                error=message,
                packages=_package_cards(app),
                max_upload=app.config["MAX_UPLOAD_BYTES"],
            ),
            413,
        )

    @app.errorhandler(404)
    def not_found(_exc):
        if _wants_json():
            return jsonify({"error": "资源不存在"}), 404
        return (
            render_template(
                "index.html",
                error="请求的资源不存在。",
                packages=_package_cards(app),
                max_upload=app.config["MAX_UPLOAD_BYTES"],
            ),
            404,
        )

    @app.after_request
    def _security_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        return response

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
