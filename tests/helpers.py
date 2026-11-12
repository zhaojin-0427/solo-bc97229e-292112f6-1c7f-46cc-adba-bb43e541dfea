"""Shared test helpers."""

from __future__ import annotations

import gzip
from pathlib import Path


def fastq_record(
    pair_id: str,
    end: str = "1",
    sequence: str = "ACGT",
    quality: str | None = None,
    comment: str = "",
) -> bytes:
    if quality is None:
        quality = "I" * len(sequence)
    suffix = f" {comment}" if comment else ""
    return f"@{pair_id}/{end}{suffix}\n{sequence}\n+\n{quality}\n".encode("utf-8")


def write_fastq_gz(path: Path, records: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=6
        ) as gz:
            for record in records:
                gz.write(record)


def corrupt_gzip_crc(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[-8] ^= 0x01
    path.write_bytes(bytes(data))


def read_fastq_gz(path: Path) -> list[tuple[str, str, str]]:
    records = []
    with gzip.open(path, "rb") as gz:
        while True:
            header = gz.readline()
            if header == b"":
                break
            sequence = gz.readline().rstrip(b"\n").decode()
            plus = gz.readline().rstrip(b"\n").decode()
            quality = gz.readline().rstrip(b"\n").decode()
            pair_id = header.decode().split()[0]
            if pair_id.startswith("@"):
                pair_id = pair_id[1:]
            if pair_id.endswith(("/1", "/2")):
                pair_id = pair_id[:-2]
            records.append((pair_id, sequence, quality))
    return records


def zip_records(path: Path) -> list[bytes]:
    return path.read_bytes().splitlines(keepends=True)
