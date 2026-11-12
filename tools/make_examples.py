#!/usr/bin/env python3
"""Generate the small FASTQ fixtures shipped in examples/.

Run from the repository root:
    python3 tools/make_examples.py
"""

from __future__ import annotations

import gzip
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "examples"


def _record(pair_id: str, end: str, sequence: str, comment: str = "") -> bytes:
    quality = "I" * len(sequence)
    suffix = f" {comment}" if comment else ""
    return f"@{pair_id}/{end}{suffix}\n{sequence}\n+\n{quality}\n".encode("ascii")


def _write_gzip(path: Path, records: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=9
        ) as gz:
            for record in records:
                gz.write(record)


def _corrupt_crc_at_tail(path: Path) -> None:
    data = bytearray(path.read_bytes())
    if len(data) < 18:
        raise RuntimeError(f"gzip file unexpectedly small: {path}")
    # The final 8 bytes are CRC32 + ISIZE.  Flipping a CRC byte leaves the
    # complete payload readable and makes the error appear only after the last
    # record has been consumed.
    data[-8] ^= 0x01
    path.write_bytes(bytes(data))


def _sequence(pair_id: str, side: str, length: int) -> str:
    alphabet = "ACGT"
    seed = hashlib.sha256(f"{pair_id}:{side}".encode("ascii")).digest()
    return "".join(alphabet[byte % 4] for byte in seed[:length])


def build_valid_disordered() -> None:
    base = ROOT / "valid-disordered"
    lengths = {"DEMO-01": 8, "DEMO-02": 7, "DEMO-03": 6, "DEMO-04": 4}
    r1_order = ["DEMO-03", "DEMO-01", "DEMO-04", "DEMO-02"]
    r2_order = ["DEMO-02", "DEMO-04", "DEMO-01", "DEMO-03"]
    r1 = [
        _record(
            pair_id,
            "1",
            _sequence(pair_id, "1", lengths[pair_id]),
            "run=SYNTHETIC lane=1",
        )
        for pair_id in r1_order
    ]
    r2 = [
        _record(
            pair_id,
            "2",
            _sequence(pair_id, "2", lengths[pair_id]),
            "run=SYNTHETIC lane=2",
        )
        for pair_id in r2_order
    ]
    _write_gzip(base / "R1.fastq.gz", r1)
    _write_gzip(base / "R2.fastq.gz", r2)
    # Keep a human-readable preview next to the uploadable gzip files.
    (base / "preview.txt").write_text(
        "R1 input order: " + ", ".join(r1_order) + "\n"
        "R2 input order: " + ", ".join(r2_order) + "\n"
        "Expected synchronized output order: " + ", ".join(r1_order) + "\n"
        "All records have four lines; headers use /1 and /2.\n",
        encoding="utf-8",
    )


def build_tail_missing_pair() -> None:
    base = ROOT / "tail-missing-pair"
    r1_ids = ["TAIL-01", "TAIL-02", "TAIL-03"]
    r2_ids = ["TAIL-01", "TAIL-02"]  # TAIL-03 is missing; reflection happens at R2 EOF.
    _write_gzip(
        base / "R1.fastq.gz",
        [
            _record(pair_id, "1", _sequence(pair_id, "1", 6))
            for pair_id in r1_ids
        ],
    )
    _write_gzip(
        base / "R2.fastq.gz",
        [
            _record(pair_id, "2", _sequence(pair_id, "2", 6))
            for pair_id in r2_ids
        ],
    )
    (base / "preview.txt").write_text(
        "R1 IDs: " + ", ".join(r1_ids) + "\n"
        "R2 IDs: " + ", ".join(r2_ids) + "\n"
        "Expected: failure at end of R2; R1 has TAIL-03 without an R2 mate.\n"
        "No package is published.\n",
        encoding="utf-8",
    )


def build_tail_corrupt_gzip() -> None:
    base = ROOT / "tail-corrupt-gzip"
    ids = ["CRC-01", "CRC-02", "CRC-03"]
    _write_gzip(
        base / "R1.fastq.gz",
        [_record(pair_id, "1", _sequence(pair_id, "1", 6)) for pair_id in ids],
    )
    r2_path = base / "R2.fastq.gz"
    _write_gzip(
        r2_path,
        [_record(pair_id, "2", _sequence(pair_id, "2", 6)) for pair_id in ids],
    )
    _corrupt_crc_at_tail(r2_path)
    (base / "preview.txt").write_text(
        "R1 and R2 IDs match, but R2's gzip CRC trailer is intentionally corrupt.\n"
        "Expected: all R2 records are readable, then a CRC error is found at EOF.\n"
        "No package is published.\n",
        encoding="utf-8",
    )


def main() -> None:
    build_valid_disordered()
    build_tail_missing_pair()
    build_tail_corrupt_gzip()
    print(f"Examples written to {ROOT}")


if __name__ == "__main__":
    main()
