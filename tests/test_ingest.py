from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path

from tests.helpers import (
    corrupt_gzip_crc,
    fastq_record,
    read_fastq_gz,
    write_fastq_gz,
)
from ingest import IngestError, run_ingest


class IngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.input_dir = self.root / "input"
        self.output_dir = self.root / "output"
        self.input_dir.mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self):
        return run_ingest(
            self.input_dir / "R1.fastq.gz",
            self.input_dir / "R2.fastq.gz",
            self.output_dir,
        )

    def test_disordered_inputs_are_output_in_r1_order_and_paired(self) -> None:
        r1_order = ["p3", "p1", "p4", "p2"]
        r2_order = ["p2", "p4", "p1", "p3"]
        r1_seq = {"p1": "AA", "p2": "CCCC", "p3": "GGGGGG", "p4": "TTTTTTTT"}
        r2_seq = {"p1": "AC", "p2": "GTGT", "p3": "TATATA", "p4": "CGCGCGCG"}
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [
                fastq_record(pid, "1", r1_seq[pid], comment="lane=1")
                for pid in r1_order
            ],
        )
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [
                fastq_record(pid, "2", r2_seq[pid], comment="lane=2")
                for pid in r2_order
            ],
        )

        result = self._run()
        self.assertEqual(result["read_count"], 4)
        self.assertTrue((self.output_dir / "R1.fastq.gz").is_file())
        self.assertTrue((self.output_dir / "R2.fastq.gz").is_file())
        out1 = read_fastq_gz(self.output_dir / "R1.fastq.gz")
        out2 = read_fastq_gz(self.output_dir / "R2.fastq.gz")
        self.assertEqual([x[0] for x in out1], r1_order)
        self.assertEqual([x[0] for x in out2], r1_order)
        for pair_id, seq, qual in out1:
            self.assertEqual(seq, r1_seq[pair_id])
            self.assertEqual(len(seq), len(qual))
        for pair_id, seq, qual in out2:
            self.assertEqual(seq, r2_seq[pair_id])
            self.assertEqual(len(seq), len(qual))

    def test_pair_id_strips_suffix_and_uses_first_whitespace_token(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [
                b"@read-A/1\tinstrument:foo\nACGT\n+\nIIII\n",
                b"@read-B extra fields\nAA\n+\nII\n",
            ],
        )
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [
                b"@read-B/2 x y z\nTT\n+\nII\n",
                b"@read-A/2\tlane\nGGGG\n+\nIIII\n",
            ],
        )
        result = self._run()
        self.assertEqual(result["read_count"], 2)
        self.assertEqual(
            [x[0] for x in read_fastq_gz(self.output_dir / "R1.fastq.gz")],
            ["read-A", "read-B"],
        )

    def test_missing_pair_discovered_at_r2_end_publishes_nothing(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [fastq_record(f"p{i}", "1") for i in range(1, 4)],
        )
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [fastq_record(f"p{i}", "2") for i in range(1, 3)],
        )
        with self.assertRaisesRegex(IngestError, "缺配"):
            self._run()
        self.assertFalse((self.output_dir / "R1.fastq.gz").exists())
        self.assertFalse((self.output_dir / "R2.fastq.gz").exists())

    def test_corrupt_gzip_tail_publishes_nothing(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [fastq_record(f"p{i}", "1") for i in range(1, 4)],
        )
        r2_path = self.input_dir / "R2.fastq.gz"
        write_fastq_gz(r2_path, [fastq_record(f"p{i}", "2") for i in range(1, 4)])
        corrupt_gzip_crc(r2_path)
        with self.assertRaisesRegex(IngestError, "gzip 文件损坏或截断"):
            self._run()
        self.assertFalse((self.output_dir / "R1.fastq.gz").exists())
        self.assertFalse((self.output_dir / "R2.fastq.gz").exists())

    def test_duplicate_r1_id_fails(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [
                fastq_record("dup", "1", "AA"),
                fastq_record("dup", "1", "CCCC"),
            ],
        )
        write_fastq_gz(self.input_dir / "R2.fastq.gz", [fastq_record("dup", "2")])
        with self.assertRaisesRegex(IngestError, "R1 中配对 ID 重复"):
            self._run()

    def test_duplicate_r2_id_fails(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [fastq_record("dup", "1", "AA")],
        )
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [
                fastq_record("dup", "2", "TT"),
                fastq_record("dup", "2", "GGGG"),
            ],
        )
        with self.assertRaisesRegex(IngestError, "R2 中配对 ID 重复"):
            self._run()

    def test_sequence_quality_length_mismatch_fails(self) -> None:
        write_fastq_gz(
            self.input_dir / "R1.fastq.gz",
            [fastq_record("p1", "1", "ACGT", quality="III")],
        )
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [fastq_record("p1", "2")],
        )
        with self.assertRaisesRegex(IngestError, "长度"):
            self._run()

    def test_truncated_last_record_fails(self) -> None:
        with open(self.input_dir / "R1.fastq.gz", "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
                gz.write(b"@p1/1\nAAAA\n+\nIIII\n@p2/1\nCC\n")
        write_fastq_gz(
            self.input_dir / "R2.fastq.gz",
            [fastq_record("p1", "2"), fastq_record("p2", "2")],
        )
        with self.assertRaisesRegex(IngestError, "记录不完整"):
            self._run()

    def test_non_gzip_input_fails(self) -> None:
        (self.input_dir / "R1.fastq.gz").write_bytes(b"not gzip\n")
        write_fastq_gz(self.input_dir / "R2.fastq.gz", [fastq_record("p1", "2")])
        with self.assertRaisesRegex(IngestError, "gzip 文件损坏或截断"):
            self._run()

    def test_empty_inputs_fail(self) -> None:
        write_fastq_gz(self.input_dir / "R1.fastq.gz", [])
        write_fastq_gz(self.input_dir / "R2.fastq.gz", [])
        with self.assertRaisesRegex(IngestError, "未找到"):
            self._run()


if __name__ == "__main__":
    unittest.main()
