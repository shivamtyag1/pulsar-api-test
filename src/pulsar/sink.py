"""Rolling gzip-JSONL sink, writing locally or to S3.

One file per `rows_per_file` rows. The roll size matters more than it looks:
at 100 rows per object the largest search in the account would produce ~59,000
S3 objects, which is a listing cost, a request bill and a loader problem at
once. At 25,000 it is a couple of dozen.

Downstream routing is by *filename*, not by prefix, so the caller supplies a
stem carrying every routing token and this module only appends the part
number and extension.

The S3 target buffers a whole part in memory before putting it. That is
deliberate rather than streaming: a part is ~10MB gzipped, a single
`put_object` is atomic, and a half-written multipart upload that a reader
could observe is exactly the failure staging-then-publish exists to prevent.
"""

from __future__ import annotations

import gzip
import io
import json
import pathlib
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterator

# MEASURED, not assumed: a real Raisin Bran export averaged 11,986 bytes per
# row uncompressed (2,042 gzipped, 5.9x). Earlier drafts assumed ~2 KB/row,
# which was 6x optimistic - these records carry full broadcast transcripts and
# 111 fields, and the largest single row seen was 385 KB.
#
# 10,000 rows therefore lands ~120 MB uncompressed / ~20 MB gzipped per object.
# At the old 25,000 an object was ~300 MB uncompressed, and since a part is
# buffered in memory before PUT that alone would exhaust a 1 GB Lambda.
BYTES_PER_ROW_ESTIMATE = 12_000
DEFAULT_ROWS_PER_FILE = 10_000


class Target(Protocol):
    """Where finished parts go. Kept minimal so tests can substitute a list."""

    def put(self, key: str, body: bytes) -> None: ...


class LocalTarget:
    """Writes into a directory tree mirroring the S3 key layout.

    Used by the operator tools so an export can be inspected before anything
    leaves the machine, and by tests.
    """

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.written: list[pathlib.Path] = []

    def put(self, key: str, body: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        self.written.append(path)


class S3Target:
    """Writes objects into one bucket.

    Content type and encoding are set per object rather than fixed, because a
    gzipped part and a plain one need different headers and getting that wrong
    leaves a reader unable to tell the bytes are compressed.

    `boto3` is imported lazily so the library, the tests and the local tools
    do not require it - only the deployed function does.
    """

    def __init__(self, bucket: str, client: Any = None) -> None:
        if client is None:
            import boto3  # lazy: not a local dependency

            client = boto3.client("s3")
        self.bucket = bucket
        self._s3 = client
        self.written: list[str] = []

    def put(self, key: str, body: bytes) -> None:
        extra: dict[str, str] = {"ContentType": "application/x-ndjson"}
        if key.endswith(".gz"):
            extra["ContentEncoding"] = "gzip"
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, **extra)
        self.written.append(key)


class RollingSink:
    """Accumulates rows and emits newline-delimited JSON parts.

    Compression is OFF by default, and that is a deliberate interop choice
    rather than a preference. The VaynerX Streaming ETL stage that ingests this
    bucket detected a sibling `.json` object within seconds while a `.jsonl.gz`
    written ten seconds earlier never reached the stage at all - and the one
    pipeline already proven through that stage (`creatoriq/`) writes plain
    `.json`. Compressing costs ~6x less storage but is worth nothing if the
    loader never sees the file.

    Set `compress=True` only once the bucket's event notification is known to
    fire on `.gz`.

    Usage::

        with RollingSink(target, prefix="pulsar/trac/...", stem="pulsar_...") as sink:
            for row in rows:
                sink.write(row)
        sink.keys  # -> the parts written

    Nothing is emitted until a part fills or the sink closes, so a caller that
    raises mid-stream leaves no partial object behind.
    """

    def __init__(
        self,
        target: Target,
        *,
        prefix: str,
        stem: str,
        rows_per_file: int = DEFAULT_ROWS_PER_FILE,
        compress: bool = False,
        start_part: int = 0,
        auto_flush: bool = True,
    ) -> None:
        if rows_per_file <= 0:
            raise ValueError("rows_per_file must be positive")
        if start_part < 0:
            raise ValueError("start_part must not be negative")
        self.target = target
        self.prefix = prefix.strip("/")
        self.stem = stem
        self.rows_per_file = rows_per_file
        self.compress = compress
        self.suffix = ".json.gz" if compress else ".json"
        self.rows_written = 0
        self.keys: list[str] = []
        self.bytes_written = 0
        self._buf: list[bytes] = []
        # A resumed export continues the part numbering where the last
        # checkpoint left it, so parts from both invocations sit side by side.
        self._part = start_part
        # With auto_flush off the CALLER decides when a part is cut - the
        # resumable export does so only at page boundaries, so a checkpoint's
        # cursor always matches exactly the rows already on S3.
        self.auto_flush = auto_flush

    @property
    def part(self) -> int:
        """The number of the last part written."""
        return self._part

    @property
    def buffered(self) -> int:
        """Rows accepted but not yet written to a part."""
        return len(self._buf)

    def write(self, row: dict[str, Any]) -> None:
        self._buf.append(json.dumps(row, separators=(",", ":")).encode() + b"\n")
        self.rows_written += 1
        if self.auto_flush and len(self._buf) >= self.rows_per_file:
            self._flush()

    def flush(self) -> None:
        """Cut a part now from whatever is buffered (no-op when empty)."""
        self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        self._part += 1
        key = f"{self.prefix}/{self.stem}_p{self._part:04d}{self.suffix}"

        payload = b"".join(self._buf)
        if self.compress:
            raw = io.BytesIO()
            # mtime=0 keeps the gzip header deterministic, so re-running an
            # export over unchanged data produces byte-identical objects and a
            # difference means the data actually differs.
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
                gz.write(payload)
            payload = raw.getvalue()
        body = payload

        self.target.put(key, body)
        self.keys.append(key)
        self.bytes_written += len(body)
        self._buf.clear()

    def close(self) -> None:
        self._flush()

    def __enter__(self) -> RollingSink:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        # Only flush a clean exit. Emitting the tail of a failed run would put
        # a short object into the prefix that looks exactly like a good one.
        if exc_type is None:
            self.close()


def iter_jsonl_gz(path: pathlib.Path) -> Iterator[dict[str, Any]]:
    """Read back a part written by this sink, compressed or not."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)
