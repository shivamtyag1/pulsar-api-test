"""The command line, driven end to end against fakes.

`PulsarClient` is swapped for the orchestrator tests' fakes, so these cover
argument handling, scope resolution, local output and the exit codes the
operator sees - without a token and without the metered API.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pytest
from test_pull import FakeData, FakeMeta

from pulsar import cli


class FakePulsarClient:
    """Context-manager stand-in routing by endpoint name."""

    fail: Exception | None = None

    def __init__(self, endpoint: str) -> None:
        self._inner: Any = FakeMeta() if endpoint == "trac_meta" else FakeData()

    def __enter__(self) -> Any:
        if FakePulsarClient.fail:
            raise FakePulsarClient.fail
        return self._inner

    def __exit__(self, *exc: object) -> None:
        return None


@pytest.fixture(autouse=True)
def fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    FakePulsarClient.fail = None
    monkeypatch.setattr(cli, "PulsarClient", FakePulsarClient)


BYPASS = ["--client", "lovesac", "--folder", "Lovesac", "--agency", "VM"]


def test_local_run_writes_the_bucket_layout(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main([*BYPASS, "--local", str(tmp_path), "--from", "2026-07-01"])
    out = capsys.readouterr().out
    assert code == 0, out
    root = tmp_path / "pulsar"
    assert sorted(p.name for p in root.iterdir()) == [
        "aggregates",
        "inventory",
        "mentions",
        "state",
    ]
    assert list((root / "mentions" / "agency=VM" / "client=lovesac").iterdir())
    assert "status:    OK" in out
    assert "manifest:" in out


def test_dry_run_writes_nothing(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main([*BYPASS, "--dry-run", "--local", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 0
    assert "estimate:" in out
    assert "Search 1" in out
    assert not any(tmp_path.iterdir())


def test_dry_run_with_one_search(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main([*BYPASS, "--dry-run", "--search-id", "2"])
    out = capsys.readouterr().out
    assert code == 0
    assert "not selected (--search-id)" in out


def test_list_folders(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["--list-folders"]) == 0
    assert "Lovesac" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--client", "Bad Name"],
        ["--client", "kotex"],  # not in the map, no override
        [*BYPASS, "--only", "everything"],
        [*BYPASS, "--limit", "500"],
    ],
)
def test_bad_input_exits_2(argv: list[str]) -> None:
    assert cli.main(argv) == 2


def test_api_failure_exits_1(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from pulsar.client import PulsarError

    FakePulsarClient.fail = PulsarError("HTTP 401 - token rejected")
    assert cli.main([*BYPASS, "--local", str(tmp_path)]) == 1
    assert "No manifest was written" in capsys.readouterr().out
    assert cli.main([*BYPASS, "--dry-run"]) == 1


def test_s3_preflight_failure_exits_2(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class NoCreds:
        def __init__(self, bucket: str) -> None:
            raise RuntimeError("Unable to locate credentials")

    monkeypatch.setattr(cli, "S3Target", NoCreds)
    assert cli.main(BYPASS) == 2
    assert "aws sso login" in capsys.readouterr().out


def test_s3_is_the_default_target(monkeypatch: pytest.MonkeyPatch) -> None:
    written: list[str] = []

    class Recorder:
        def __init__(self, bucket: str) -> None:
            self.bucket = bucket

        def put(self, key: str, body: bytes) -> None:
            written.append(key)

    monkeypatch.setattr(cli, "S3Target", Recorder)
    assert cli.main(BYPASS) == 0
    assert written[0].startswith("pulsar/state/preflight/")
    assert written[-1].endswith("manifest.json")


def test_pii_flag_is_announced(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli.main([*BYPASS, "--include-pii", "--dry-run"])
    assert "Legal/InfoSec" in capsys.readouterr().out


def test_legacy_translation() -> None:
    assert cli.translate_legacy(["--client", "x"], "mentions") == [
        "--only",
        "mentions",
        "--client",
        "x",
        "--local",
        "out/s3",
    ]
    assert cli.translate_legacy(["--client", "x", "--s3"], "mentions") == [
        "--only",
        "mentions",
        "--client",
        "x",
    ]


def test_status_line_admits_to_drops(capsys: pytest.CaptureFixture[str]) -> None:
    manifest = {
        "searches": [],
        "rollup": {"aggregate_rows": 1, "series_dropped": ["metric:REACH"]},
        "rows": {"mentions": 0, "aggregates": 1, "inventory": 0},
        "api_calls": {"trac_data": 1, "trac_meta": 1},
        "manifest_key": "pulsar/state/runs/run=x/manifest.json",
        "ok": True,
        "errors": 0,
        "dropped": 1,
    }
    cli.print_summary(manifest, "out/s3", 1.0)
    out = capsys.readouterr().out
    assert "COMPLETE, but 1 facet/series pull(s) dropped" in out
    assert "dropped: metric:REACH" in out
