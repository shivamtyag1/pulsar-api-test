"""Rolling NDJSON sink.

The behaviours worth pinning are the ones whose failure is silent: a part
emitted for a run that raised, a roll boundary off by one, an extension the
downstream loader does not recognise, or bytes that differ between identical
runs (which would make "did the data change?" unanswerable by comparing
objects).
"""

from __future__ import annotations

import gzip
import io
import json
import pathlib
from typing import Any

import pytest

from pulsar.sink import (
    DEFAULT_ROWS_PER_FILE,
    LocalTarget,
    RollingSink,
    S3Target,
    iter_jsonl_gz,
)


class FakeTarget:
    """Captures parts in memory so tests never touch a filesystem or AWS."""

    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []

    def put(self, key: str, body: bytes) -> None:
        self.puts.append((key, body))

    def rows(self, index: int) -> list[dict[str, Any]]:
        key, body = self.puts[index]
        if key.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                body = gz.read()
        return [json.loads(line) for line in body.decode().splitlines()]


def sink(target: FakeTarget, **kw: Any) -> RollingSink:
    params: dict[str, Any] = {"prefix": "pulsar/client=acme", "stem": "part"}
    params.update(kw)
    return RollingSink(target, **params)


class TestFormat:
    """The extension decides which staging procedure the loader picks - and
    whether the bucket's event notification fires at all."""

    def test_plain_json_is_the_default(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=1) as s:
            s.write({"i": 1})
        assert s.keys == ["pulsar/client=acme/part_p0001.json"]
        assert json.loads(t.puts[0][1].decode()) == {"i": 1}

    def test_compress_switches_extension_and_encodes(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=1, compress=True) as s:
            s.write({"i": 1})
        assert s.keys == ["pulsar/client=acme/part_p0001.json.gz"]
        assert t.puts[0][1][:2] == b"\x1f\x8b"  # gzip magic
        assert t.rows(0) == [{"i": 1}]

    def test_output_is_newline_delimited_not_an_array(self) -> None:
        """SP_STG_FROM_JSON loads $1 per row; an array would arrive as one row."""
        t = FakeTarget()
        with sink(t, rows_per_file=10) as s:
            for i in range(3):
                s.write({"i": i})
        body = t.puts[0][1].decode()
        assert body.count("\n") == 3
        assert not body.lstrip().startswith("[")


class TestRolling:
    def test_single_part_when_under_the_limit(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=10) as s:
            for i in range(7):
                s.write({"pulsarId": i})
        assert len(t.puts) == 1
        assert s.rows_written == 7
        assert [r["pulsarId"] for r in t.rows(0)] == list(range(7))

    def test_rolls_on_the_boundary_not_after_it(self) -> None:
        """A part holds exactly rows_per_file rows - an off-by-one here would
        silently produce objects the loader sizes its batches against wrongly."""
        t = FakeTarget()
        with sink(t, rows_per_file=3) as s:
            for i in range(7):
                s.write({"pulsarId": i})
        assert [len(t.rows(i)) for i in range(len(t.puts))] == [3, 3, 1]
        assert s.rows_written == 7

    def test_exact_multiple_emits_no_empty_trailing_part(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=3) as s:
            for i in range(6):
                s.write({"pulsarId": i})
        assert len(t.puts) == 2

    def test_no_rows_writes_nothing(self) -> None:
        t = FakeTarget()
        with sink(t) as s:
            pass
        assert t.puts == []
        assert s.keys == []

    def test_part_numbering_is_zero_padded_and_sequential(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=1) as s:
            for i in range(3):
                s.write({"i": i})
        assert s.keys == [
            "pulsar/client=acme/part_p0001.json",
            "pulsar/client=acme/part_p0002.json",
            "pulsar/client=acme/part_p0003.json",
        ]

    def test_prefix_slashes_are_normalised(self) -> None:
        t = FakeTarget()
        with sink(t, prefix="/a/b/", rows_per_file=1) as s:
            s.write({"i": 0})
        assert s.keys == ["a/b/part_p0001.json"]


class TestFailureHandling:
    def test_an_exception_suppresses_the_tail_part(self) -> None:
        """The whole point of staging-then-publish: a short object in the live
        prefix is indistinguishable from a complete one."""
        t = FakeTarget()
        with pytest.raises(RuntimeError), sink(t, rows_per_file=10) as s:
            s.write({"pulsarId": 1})
            raise RuntimeError("vendor blew up mid-stream")
        assert t.puts == []

    def test_full_parts_before_a_failure_are_already_out(self) -> None:
        """Parts that filled were flushed as they filled - that is why the
        caller must treat a failed run's prefix as untrusted, not as partial."""
        t = FakeTarget()
        with pytest.raises(RuntimeError), sink(t, rows_per_file=2) as s:
            for i in range(5):
                s.write({"i": i})
            raise RuntimeError("boom")
        assert len(t.puts) == 2  # 4 rows flushed, the 5th suppressed

    def test_rows_per_file_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            RollingSink(FakeTarget(), prefix="p", stem="s", rows_per_file=0)


class TestDeterminism:
    @pytest.mark.parametrize("compress", [False, True])
    def test_identical_input_produces_identical_bytes(self, compress: bool) -> None:
        """gzip stamps mtime by default, which would make two runs over
        unchanged data differ. Without mtime=0, comparing objects is useless."""
        out = []
        for _ in range(2):
            t = FakeTarget()
            with sink(t, rows_per_file=10, compress=compress) as s:
                for i in range(5):
                    s.write({"pulsarId": i, "likes": None})
            out.append(t.puts[0][1])
        assert out[0] == out[1]

    def test_bytes_written_matches_what_the_target_received(self) -> None:
        t = FakeTarget()
        with sink(t, rows_per_file=2) as s:
            for i in range(5):
                s.write({"i": i})
        assert s.bytes_written == sum(len(b) for _, b in t.puts)


class TestLocalTarget:
    @pytest.mark.parametrize("compress", [False, True])
    def test_writes_the_key_as_a_path_and_reads_back(
        self, tmp_path: pathlib.Path, compress: bool
    ) -> None:
        t = LocalTarget(tmp_path)
        with RollingSink(
            t, prefix="pulsar/x", stem="x", rows_per_file=2, compress=compress
        ) as s:
            for i in range(3):
                s.write({"pulsarId": i})
        ext = ".json.gz" if compress else ".json"
        assert (tmp_path / f"pulsar/x/x_p0001{ext}").exists()
        rows = [r for p in sorted(t.written) for r in iter_jsonl_gz(p)]
        assert [r["pulsarId"] for r in rows] == [0, 1, 2]


class TestS3Target:
    @staticmethod
    def _capture() -> tuple[Any, list[dict[str, Any]]]:
        calls: list[dict[str, Any]] = []

        class FakeS3:
            def put_object(self, **kw: Any) -> None:
                calls.append(kw)

        return FakeS3(), calls

    def test_plain_json_carries_no_gzip_header(self) -> None:
        """ContentEncoding: gzip on an uncompressed object makes a conforming
        reader try to inflate plain text and fail."""
        fake, calls = self._capture()
        with RollingSink(
            S3Target("vx-analytics", fake), prefix="pulsar/x", stem="s", rows_per_file=1
        ) as s:
            s.write({"i": 1})
        assert calls[0]["Key"] == "pulsar/x/s_p0001.json"
        assert calls[0]["ContentType"] == "application/x-ndjson"
        assert "ContentEncoding" not in calls[0]
        assert s.keys == ["pulsar/x/s_p0001.json"]

    def test_gzipped_object_declares_its_encoding(self) -> None:
        fake, calls = self._capture()
        with RollingSink(
            S3Target("vx-analytics", fake),
            prefix="pulsar/x",
            stem="s",
            rows_per_file=1,
            compress=True,
        ) as s:
            s.write({"i": 1})
        assert calls[0]["Key"] == "pulsar/x/s_p0001.json.gz"
        assert calls[0]["ContentEncoding"] == "gzip"


def test_default_roll_size_keeps_a_part_inside_lambda_memory() -> None:
    """Pinned because it is a memory bound, not a preference.

    A part is buffered in RAM before PUT, so rows_per_file x bytes_per_row must
    stay well under the function's memory. Measured 12 KB/row, so 10,000 rows
    is ~120 MB serialized - and live Python objects cost several times that.
    Raising this without raising `memory` in build/manifest.toml will OOM.
    """
    from pulsar.sink import BYTES_PER_ROW_ESTIMATE

    assert DEFAULT_ROWS_PER_FILE == 10_000
    part_mb = DEFAULT_ROWS_PER_FILE * BYTES_PER_ROW_ESTIMATE / 1e6
    assert part_mb < 150, f"a part is {part_mb:.0f} MB serialized"
