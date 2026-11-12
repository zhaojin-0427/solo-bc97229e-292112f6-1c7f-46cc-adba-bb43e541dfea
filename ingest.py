"""Streaming, disk-backed pairing of two gzip FASTQ files.

The algorithm deliberately keeps only one FASTQ record and a bounded amount of
SQLite page cache in memory.  Read names are indexed in a per-job SQLite
database; R2 records are appended to a temporary spool so they can be retrieved
in the order dictated by R1.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import sqlite3
import time
import zlib
from pathlib import Path
from typing import Callable, Iterator, NamedTuple

SQLITE_CACHE_KIB = 8 * 1024
COMMIT_EVERY = 50_000


class IngestError(Exception):
    """A user-facing ingest failure.  The package must not be published."""


class FastqRecord(NamedTuple):
    pair_id: bytes
    normalized: bytes


class CountingReader:
    """File wrapper that counts compressed bytes consumed by gzip."""

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        data = self._fileobj.read(size)
        self.bytes_read += len(data)
        return data

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        self._fileobj.close()


class HashingReader(CountingReader):
    def __init__(self, fileobj, hasher):
        super().__init__(fileobj)
        self._hasher = hasher

    def read(self, size: int = -1) -> bytes:
        data = super().read(size)
        self._hasher.update(data)
        return data


class HashingWriter:
    """File wrapper that hashes exactly the compressed bytes written to disk."""

    def __init__(self, fileobj, hasher):
        self._fileobj = fileobj
        self._hasher = hasher

    def write(self, data: bytes) -> int:
        self._hasher.update(data)
        return self._fileobj.write(data)

    def flush(self) -> None:
        self._fileobj.flush()

    def writable(self) -> bool:
        return True

    def close(self) -> None:
        self._fileobj.close()


def _strip_eol(line: bytes) -> bytes:
    if line.endswith(b"\n"):
        line = line[:-1]
    if line.endswith(b"\r"):
        line = line[:-1]
    return line


def _parse_pair_id(header: bytes, end_label: str, record_number: int) -> bytes:
    title = _strip_eol(header)
    if not title.startswith(b"@"):
        raise IngestError(
            f"{end_label} 第 {record_number} 条记录标题无效：应以 '@' 开头"
        )
    fields = title[1:].split(None, 1)
    if not fields:
        raise IngestError(
            f"{end_label} 第 {record_number} 条记录标题为空，无法得到配对 ID"
        )
    pair_id = fields[0]
    if pair_id.endswith(b"/1") or pair_id.endswith(b"/2"):
        pair_id = pair_id[:-2]
    if not pair_id:
        raise IngestError(
            f"{end_label} 第 {record_number} 条记录去除 /1 或 /2 后配对 ID 为空"
        )
    return pair_id


def _display_id(pair_id: bytes, limit: int = 160) -> str:
    text = pair_id.decode("utf-8", errors="replace")
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


class FastqReader:
    """Read a four-line gzip FASTQ stream without loading it into memory.

    Gzip CRC/EOF errors are surfaced as IngestError.  The reader is also able
    to hash the compressed bytes it consumes, which lets the caller verify a
    freshly written output without an additional compression pass.
    """

    def __init__(
        self,
        path: os.PathLike[str] | str,
        end_label: str,
        hasher=None,
    ):
        self.path = os.fspath(path)
        self.end_label = end_label
        self._raw = open(self.path, "rb")
        self._hasher = hasher
        if hasher is None:
            self._counter = CountingReader(self._raw)
        else:
            self._counter = HashingReader(self._raw, hasher)
        try:
            self._gz = gzip.GzipFile(fileobj=self._counter, mode="rb")
        except Exception:
            self._raw.close()
            raise
        self.compressed_size = max(os.path.getsize(self.path), 1)

    @property
    def compressed_bytes_read(self) -> int:
        return self._counter.bytes_read

    def __enter__(self) -> "FastqReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._gz.close()
        except (OSError, EOFError, zlib.error, gzip.BadGzipFile) as exc:
            raise IngestError(
                f"{self.end_label} gzip 文件损坏或截断"
                f"（{type(exc).__name__}: {exc}）"
            ) from exc
        finally:
            self._raw.close()

    def records(self) -> Iterator[FastqRecord]:
        record_number = 0
        try:
            while True:
                header = self._gz.readline()
                if header == b"":
                    break
                record_number += 1
                sequence = self._gz.readline()
                plus = self._gz.readline()
                quality = self._gz.readline()

                if sequence == b"":
                    raise IngestError(
                        f"{self.end_label} 第 {record_number} 条 FASTQ 记录不完整："
                        "缺少序列行（文件末尾被截断）"
                    )
                if plus == b"":
                    raise IngestError(
                        f"{self.end_label} 第 {record_number} 条 FASTQ 记录不完整："
                        "缺少 '+' 行（文件末尾被截断）"
                    )
                if quality == b"":
                    raise IngestError(
                        f"{self.end_label} 第 {record_number} 条 FASTQ 记录不完整："
                        "缺少质量行（文件末尾被截断）"
                    )

                pair_id = _parse_pair_id(header, self.end_label, record_number)
                seq = _strip_eol(sequence)
                plus_line = _strip_eol(plus)
                qual = _strip_eol(quality)
                if not plus_line.startswith(b"+"):
                    raise IngestError(
                        f"{self.end_label} 第 {record_number} 条记录第三行"
                        "不是以 '+' 开头的 FASTQ 分隔行"
                    )
                if len(seq) != len(qual):
                    raise IngestError(
                        f"{self.end_label} 第 {record_number} 条记录序列长度 "
                        f"{len(seq)} 与质量长度 {len(qual)} 不相等"
                    )

                normalized = b"\n".join(
                    (_strip_eol(header), seq, plus_line, qual, b"")
                )
                yield FastqRecord(pair_id, normalized)
        except IngestError:
            raise
        except (OSError, EOFError, zlib.error, gzip.BadGzipFile) as exc:
            raise IngestError(
                f"{self.end_label} gzip 文件损坏或截断"
                f"（{type(exc).__name__}: {exc}）"
            ) from exc


def _configure_index(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_KIB}")
    conn.execute(
        """
        CREATE TABLE r1 (
            id BLOB PRIMARY KEY,
            seq INTEGER NOT NULL UNIQUE
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        """
        CREATE TABLE r2 (
            id BLOB PRIMARY KEY,
            seq INTEGER NOT NULL,
            off INTEGER NOT NULL,
            len INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )


def _missing_ids(conn: sqlite3.Connection, left: str, right: str, limit: int = 8):
    # Table names are internal constants, never user input.
    rows = conn.execute(
        f"""
        SELECT l.id
        FROM {left} AS l
        LEFT JOIN {right} AS r ON r.id = l.id
        WHERE r.id IS NULL
        LIMIT {limit}
        """
    ).fetchall()
    return [row[0] for row in rows]


def _check_pair_sets(
    conn: sqlite3.Connection, r1_count: int, r2_count: int
) -> None:
    r1_missing_r2 = _missing_ids(conn, "r1", "r2")
    r2_missing_r1 = _missing_ids(conn, "r2", "r1")
    if not r1_missing_r2 and not r2_missing_r1 and r1_count == r2_count:
        return

    parts = [f"两端配对 ID 集合不一致：R1 有 {r1_count} 条，R2 有 {r2_count} 条"]
    if r1_missing_r2:
        examples = "、".join(_display_id(x) for x in r1_missing_r2)
        parts.append(f"R1 中在 R2 缺配的 ID（示例）：{examples}")
    if r2_missing_r1:
        examples = "、".join(_display_id(x) for x in r2_missing_r1)
        parts.append(f"R2 中在 R1 缺配的 ID（示例）：{examples}")
    if r1_count == r2_count and not r1_missing_r2 and not r2_missing_r1:
        parts.append("计数相同但集合校验失败")
    raise IngestError("；".join(parts))


def run_ingest(
    r1_path: os.PathLike[str] | str,
    r2_path: os.PathLike[str] | str,
    work_dir: os.PathLike[str] | str,
    progress: Callable[[str, int, int, int], None] | None = None,
) -> dict:
    """Validate and pair two gzip FASTQ inputs.

    Outputs ``R1.fastq.gz`` and ``R2.fastq.gz`` in ``work_dir``.  The R1 output
    preserves the input R1 order.  Nothing is written outside ``work_dir`` and
    no publication happens here.
    """

    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    index_path = work / "pairs.sqlite3"
    r2_spool_path = work / "r2.records"
    out_r1_path = work / "R1.fastq.gz"
    out_r2_path = work / "R2.fastq.gz"

    for stale in (index_path, r2_spool_path, out_r1_path, out_r2_path):
        try:
            stale.unlink()
        except FileNotFoundError:
            pass

    state = {"r1_count": 0, "r2_count": 0}
    last_emit = {"time": 0.0, "stage": "", "percent": -1}

    def emit(stage: str, percent: int, force: bool = False) -> None:
        if progress is None:
            return
        percent = max(0, min(100, int(percent)))
        now = time.monotonic()
        if (
            force
            or stage != last_emit["stage"]
            or now - last_emit["time"] >= 1.0
            or (
                percent != last_emit["percent"]
                and now - last_emit["time"] >= 0.25
            )
        ):
            last_emit.update(time=now, stage=stage, percent=percent)
            progress(stage, percent, state["r1_count"], state["r2_count"])

    emit("校验 R1 并建立配对索引", 1, force=True)
    conn = sqlite3.connect(str(index_path), isolation_level=None)
    _configure_index(conn)
    conn.execute("BEGIN")

    try:
        # Pass 1a: validate R1 and remember every ID exactly once.
        with FastqReader(r1_path, "R1") as reader:
            r1_size = reader.compressed_size
            for record in reader.records():
                state["r1_count"] += 1
                seq_index = state["r1_count"] - 1
                try:
                    conn.execute(
                        "INSERT INTO r1(id, seq) VALUES(?, ?)",
                        (record.pair_id, seq_index),
                    )
                except sqlite3.IntegrityError as exc:
                    first = conn.execute(
                        "SELECT seq FROM r1 WHERE id = ?", (record.pair_id,)
                    ).fetchone()
                    first_number = (first[0] + 1) if first else "未知"
                    raise IngestError(
                        "R1 中配对 ID 重复："
                        f"{_display_id(record.pair_id)}"
                        f"（第 {first_number} 条与第 {seq_index + 1} 条）"
                    ) from exc
                if state["r1_count"] % COMMIT_EVERY == 0:
                    conn.commit()
                    conn.execute("BEGIN")
                emit(
                    "校验 R1 并建立配对索引",
                    1 + int(reader.compressed_bytes_read / r1_size * 34),
                )
        conn.commit()
        conn.execute("BEGIN")
        emit("校验 R1 并建立配对索引", 35, force=True)

        # Pass 1b: validate R2, spool the exact records, and index their offsets.
        with open(r2_spool_path, "wb") as spool:
            with FastqReader(r2_path, "R2") as reader:
                r2_size = reader.compressed_size
                for record in reader.records():
                    state["r2_count"] += 1
                    seq_index = state["r2_count"] - 1
                    offset = spool.tell()
                    try:
                        conn.execute(
                            "INSERT INTO r2(id, seq, off, len) VALUES(?, ?, ?, ?)",
                            (
                                record.pair_id,
                                seq_index,
                                offset,
                                len(record.normalized),
                            ),
                        )
                    except sqlite3.IntegrityError as exc:
                        first = conn.execute(
                            "SELECT seq FROM r2 WHERE id = ?", (record.pair_id,)
                        ).fetchone()
                        first_number = (first[0] + 1) if first else "未知"
                        raise IngestError(
                            "R2 中配对 ID 重复："
                            f"{_display_id(record.pair_id)}"
                            f"（第 {first_number} 条与第 {seq_index + 1} 条）"
                        ) from exc
                    spool.write(record.normalized)
                    if state["r2_count"] % COMMIT_EVERY == 0:
                        conn.commit()
                        conn.execute("BEGIN")
                    emit(
                        "校验 R2 并建立配对索引",
                        35 + int(reader.compressed_bytes_read / r2_size * 33),
                    )
        conn.commit()
        conn.execute("BEGIN")
        emit("校验 R2 并建立配对索引", 68, force=True)

        if state["r1_count"] == 0 or state["r2_count"] == 0:
            raise IngestError(
                "未找到可处理的 FASTQ 记录："
                f"R1 {state['r1_count']} 条，R2 {state['r2_count']} 条"
            )

        emit("检查两端配对", 69, force=True)
        _check_pair_sets(conn, state["r1_count"], state["r2_count"])
        conn.commit()
        emit("检查两端配对", 70, force=True)

        # Pass 2: re-stream R1 in its original order, fetch matching R2 records
        # from the spool, and emit both outputs synchronously.
        emit("按 R1 顺序写出文件", 70, force=True)
        r1_hasher = hashlib.sha256()
        r2_hasher = hashlib.sha256()
        written = 0
        out1_raw = open(out_r1_path, "wb")
        out2_raw = open(out_r2_path, "wb")
        writer1 = HashingWriter(out1_raw, r1_hasher)
        writer2 = HashingWriter(out2_raw, r2_hasher)
        gz1 = gzip.GzipFile(
            filename="", mode="wb", fileobj=writer1, mtime=0, compresslevel=6
        )
        gz2 = gzip.GzipFile(
            filename="", mode="wb", fileobj=writer2, mtime=0, compresslevel=6
        )
        try:
            with FastqReader(r1_path, "R1") as reader, open(
                r2_spool_path, "rb"
            ) as spool:
                r1_size = reader.compressed_size
                lookup = conn.cursor()
                for record in reader.records():
                    lookup.execute(
                        "SELECT off, len FROM r2 WHERE id = ?", (record.pair_id,)
                    )
                    row = lookup.fetchone()
                    if row is None:
                        raise IngestError(
                            "内部配对索引不一致：R1 ID 在 R2 索引中不存在："
                            f"{_display_id(record.pair_id)}"
                        )
                    spool.seek(row[0])
                    r2_record = spool.read(row[1])
                    if len(r2_record) != row[1]:
                        raise IngestError(
                            "读取 R2 暂存记录失败：预期 "
                            f"{row[1]} 字节，实际 {len(r2_record)} 字节"
                        )
                    gz1.write(record.normalized)
                    gz2.write(r2_record)
                    written += 1
                    emit(
                        "按 R1 顺序写出文件",
                        70 + int(reader.compressed_bytes_read / r1_size * 22),
                    )
        finally:
            gz1.close()
            gz2.close()
            out1_raw.flush()
            out2_raw.flush()
            os.fsync(out1_raw.fileno())
            os.fsync(out2_raw.fileno())
            out1_raw.close()
            out2_raw.close()

        if written != state["r1_count"]:
            raise IngestError(
                f"写出记录数不一致：预期 {state['r1_count']} 条，实际 {written} 条"
            )
        emit("按 R1 顺序写出文件", 92, force=True)

        # Pass 3: decompress both products and verify CRC, structure and pair
        # synchronization.  Hashes are taken from the same compressed reads.
        verify1_hasher = hashlib.sha256()
        verify2_hasher = hashlib.sha256()
        emit("校验输出 gzip", 93, force=True)
        with (
            FastqReader(out_r1_path, "输出 R1", verify1_hasher) as reader1,
            FastqReader(out_r2_path, "输出 R2", verify2_hasher) as reader2,
        ):
            iterator1 = reader1.records()
            iterator2 = reader2.records()
            verified = 0
            while True:
                try:
                    record1 = next(iterator1)
                except StopIteration:
                    record1 = None
                try:
                    record2 = next(iterator2)
                except StopIteration:
                    record2 = None
                if record1 is None and record2 is None:
                    break
                if record1 is None or record2 is None:
                    raise IngestError(
                        "输出校验失败：R1 与 R2 记录数不同"
                    )
                verified += 1
                if record1.pair_id != record2.pair_id:
                    raise IngestError(
                        "输出校验失败：R1 与 R2 第 "
                        f"{verified} 条记录不同步（"
                        f"{_display_id(record1.pair_id)} / "
                        f"{_display_id(record2.pair_id)}）"
                    )
                if verified % 50_000 == 0:
                    emit(
                        "校验输出 gzip",
                        93
                        + int(
                            reader1.compressed_bytes_read / reader1.compressed_size * 6
                        ),
                    )
            if verified != state["r1_count"]:
                raise IngestError(
                    f"输出校验失败：预期 {state['r1_count']} 条，实际 {verified} 条"
                )
        emit("校验输出 gzip", 99, force=True)

        if verify1_hasher.digest() != r1_hasher.digest():
            raise IngestError("输出校验失败：R1 写入校验和与复读校验和不一致")
        if verify2_hasher.digest() != r2_hasher.digest():
            raise IngestError("输出校验失败：R2 写入校验和与复读校验和不一致")

        return {
            "read_count": state["r1_count"],
            "r1_path": str(out_r1_path),
            "r2_path": str(out_r2_path),
            "r1_sha256": r1_hasher.hexdigest(),
            "r2_sha256": r2_hasher.hexdigest(),
            "r1_size": os.path.getsize(out_r1_path),
            "r2_size": os.path.getsize(out_r2_path),
        }
    finally:
        try:
            conn.close()
        finally:
            try:
                index_path.unlink()
            except FileNotFoundError:
                pass
            try:
                r2_spool_path.unlink()
            except FileNotFoundError:
                pass
