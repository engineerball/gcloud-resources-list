"""Tests for registry, writer, and runner — no GCP access."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import cast

import pytest
from google.auth.credentials import Credentials

import gcp_inventory.runner as runner_module
from gcp_inventory.config import Config
from gcp_inventory.registry import (
    _REGISTRY,  # noqa: PLC2701 - test-only access
    CollectorContext,
    CollectorSelectionError,
    CollectorSpec,
    RowIterator,
    Scope,
    collector,
    select_collectors,
)
from gcp_inventory.runner import (
    MAX_ATTEMPTS,
    _collect_rows,  # noqa: PLC2701 - test-only access
    build_tasks,
    is_transient,
    run,
    service_disabled_reason,
    vpc_service_controls_reason,
)
from gcp_inventory.writer import RunWriter, sanitize_csv_cell

FAKE_CREDS = cast(Credentials, object())
CONFIG = Config(projects=("p1", "p2"), locations=("r1", "r2"))


def spec_of(fn, name="s", scope=Scope.GLOBAL, header=("Project", "Value")) -> CollectorSpec:
    return CollectorSpec(name=name, filename=f"{name}.csv", header=header, scope=scope, fn=fn)


def read_csv(path: Path) -> list[list[str]]:
    with path.open() as fh:
        return list(csv.reader(fh))


# --- registry ---------------------------------------------------------------


def test_collector_decorator_registers_and_rejects_duplicates() -> None:
    @collector("test_dup", "test_dup.csv", ("A",), Scope.GLOBAL)
    def fn(ctx: CollectorContext) -> RowIterator:
        yield ("x",)

    try:
        assert _REGISTRY["test_dup"].filename == "test_dup.csv"
        with pytest.raises(ValueError, match="Duplicate"):
            collector("test_dup", "other.csv", ("A",), Scope.GLOBAL)(fn)
    finally:
        del _REGISTRY["test_dup"]


def test_parent_requires_location() -> None:
    ctx = CollectorContext("p", None, FAKE_CREDS)
    with pytest.raises(RuntimeError, match="GLOBAL"):
        _ = ctx.parent
    assert CollectorContext("p", "r", FAKE_CREDS).parent == "projects/p/locations/r"


def test_select_collectors_preserves_registration_order_and_removes_duplicates() -> None:
    specs = select_collectors(
        ["cloud_storage_buckets", "service_accounts", "cloud_storage_buckets"]
    )

    assert [spec.name for spec in specs] == ["service_accounts", "cloud_storage_buckets"]


def test_select_collectors_rejects_unknown_names_with_valid_options() -> None:
    with pytest.raises(CollectorSelectionError) as excinfo:
        select_collectors(["service_accounts", "typo"])

    message = str(excinfo.value)
    assert "typo" in message
    assert "service_accounts" in message
    assert "cloud_storage_buckets" in message


# --- task fan-out -----------------------------------------------------------


def test_build_tasks_fanout() -> None:
    def fn(ctx: CollectorContext) -> RowIterator:
        yield from ()

    tasks = build_tasks(
        [spec_of(fn, "g", Scope.GLOBAL), spec_of(fn, "r", Scope.REGIONAL)], CONFIG, FAKE_CREDS
    )
    keys = [(s.name, c.project, c.location) for s, c in tasks]
    assert keys == [
        ("g", "p1", None),
        ("g", "p2", None),
        ("r", "p1", "r1"),
        ("r", "p1", "r2"),
        ("r", "p2", "r1"),
        ("r", "p2", "r2"),
    ]


# --- writer -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("=HYPERLINK(\"https://example.invalid\")", "'=HYPERLINK(\"https://example.invalid\")"),
        ("+formula", "'+formula"),
        ("-formula", "'-formula"),
        ("@formula", "'@formula"),
        ("\tformula", "'\tformula"),
        ("\rformula", "' formula"),
        ("\n=1+1", "' =1+1"),
        ("\n\n=cmd|'/c calc'!A0", "' =cmd|'/c calc'!A0"),
        (" =1+1", "' =1+1"),
        ("\t=1+1", "'\t=1+1"),
        ("-42", "-42"),
        ("line one\r\nline two", "line one line two"),
        (-42, -42),
    ],
)
def test_sanitize_csv_cell(value: object, expected: object) -> None:
    assert sanitize_csv_cell(value) == expected


@pytest.mark.parametrize(
    "value", ["\n=1+1", "\n\n=cmd|'/c calc'!A0", " =1+1", "\t=1+1"]
)
def test_sanitize_csv_cell_neutralizes_hidden_formula_prefix(value: str) -> None:
    result = sanitize_csv_cell(value)
    assert isinstance(result, str)
    assert not result.lstrip().startswith(("=", "+", "-", "@"))


def test_writer_sanitizes_every_text_cell(tmp_path: Path) -> None:
    writer = RunWriter(tmp_path, "run1")
    count = writer.write(
        spec_of(lambda ctx: iter(()), header=("=Header", "Value")),
        [("=cmd|'/c calc'!A0", "line one\r\nline two")],
    )

    final = writer.finalize()

    assert count == 1
    assert read_csv(final / "s.csv") == [
        ["'=Header", "Value"],
        ["'=cmd|'/c calc'!A0", "line one line two"],
    ]


def test_writer_single_header_across_tasks_and_atomic_publish(tmp_path: Path) -> None:
    def fn(ctx: CollectorContext) -> RowIterator:
        yield (ctx.project, "v")

    spec = spec_of(fn)
    writer = RunWriter(tmp_path, "run1")
    writer.write(spec, [("p1", "a")])
    writer.write(spec, [("p2", "b")])
    assert not (tmp_path / "run1").exists()  # nothing published yet

    final = writer.finalize()
    assert final == tmp_path / "run1"
    assert not (tmp_path / ".in-progress-run1").exists()
    assert read_csv(final / "s.csv") == [["Project", "Value"], ["p1", "a"], ["p2", "b"]]
    assert (tmp_path / "latest").resolve() == final.resolve()


def test_writer_rejects_duplicate_run_id(tmp_path: Path) -> None:
    RunWriter(tmp_path, "run1")
    with pytest.raises(FileExistsError):
        RunWriter(tmp_path, "run1")


def test_writer_latest_symlink_moves_to_newest_run(tmp_path: Path) -> None:
    def make_run(run_id: str) -> None:
        w = RunWriter(tmp_path, run_id)
        w.write(spec_of(lambda ctx: iter(())), [])
        w.finalize()

    make_run("run1")
    make_run("run2")
    assert (tmp_path / "latest").resolve() == (tmp_path / "run2").resolve()


def test_writer_rejects_real_directory_at_latest(tmp_path: Path) -> None:
    (tmp_path / "latest").mkdir()
    writer = RunWriter(tmp_path, "run1")

    with pytest.raises(FileExistsError, match="not a symlink"):
        writer.finalize()

    assert (tmp_path / "latest").is_dir()
    assert (tmp_path / "run1").is_dir()
    assert not writer.staging_dir.exists()


# --- runner isolation -------------------------------------------------------


def test_run_isolates_failures_and_writes_successes(tmp_path: Path) -> None:
    def good(ctx: CollectorContext) -> RowIterator:
        yield (ctx.project, "ok")

    def raises_immediately(ctx: CollectorContext) -> RowIterator:
        raise RuntimeError("boom")
        yield  # pragma: no cover

    def raises_mid_iteration(ctx: CollectorContext) -> RowIterator:
        yield (ctx.project, "partial")
        raise PermissionError("denied")

    specs = [
        spec_of(good, "good"),
        spec_of(raises_immediately, "immediate"),
        spec_of(raises_mid_iteration, "midway"),
    ]
    config = Config(projects=("p1", "p2"), locations=("r1",))
    writer = RunWriter(tmp_path, "run1")
    results = run(specs, config, FAKE_CREDS, writer)
    final = writer.finalize()

    by_name = {}
    for r in results:
        by_name.setdefault(r.collector, []).append(r)

    assert all(r.ok and r.rows == 1 for r in by_name["good"])
    assert all(not r.ok and "boom" in str(r.error) for r in by_name["immediate"])
    assert all(not r.ok and r.rows == 1 and "denied" in str(r.error) for r in by_name["midway"])
    assert len(results) == 6  # 3 specs × 2 projects, no aborts

    assert read_csv(final / "good.csv") == [["Project", "Value"], ["p1", "ok"], ["p2", "ok"]]
    # failing collector that yielded before raising: partial rows are kept
    assert read_csv(final / "midway.csv") == [
        ["Project", "Value"],
        ["p1", "partial"],
        ["p2", "partial"],
    ]
    # collector that raised before yielding anything: header-only file
    assert read_csv(final / "immediate.csv") == [["Project", "Value"]]


def test_run_bounds_in_flight_tasks_and_preserves_write_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TrackingFuture:
        def __init__(self, executor, fn, args) -> None:
            self.executor = executor
            self.fn = fn
            self.args = args

        def result(self):
            try:
                return self.fn(*self.args)
            finally:
                self.executor.outstanding -= 1

    class TrackingExecutor:
        last = None

        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers
            self.outstanding = 0
            self.peak_outstanding = 0
            TrackingExecutor.last = self

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def submit(self, fn, *args):
            self.outstanding += 1
            self.peak_outstanding = max(self.peak_outstanding, self.outstanding)
            return TrackingFuture(self, fn, args)

    class RecordingWriter:
        def __init__(self) -> None:
            self.projects: list[str] = []

        def write(self, spec, rows) -> None:
            self.projects.extend(row[0] for row in rows)

    monkeypatch.setattr(runner_module, "ThreadPoolExecutor", TrackingExecutor)
    projects = tuple(f"p{index}" for index in range(10))
    config = Config(projects=projects, locations=("r1",))
    writer = RecordingWriter()

    run(
        [spec_of(lambda ctx: iter(((ctx.project, "ok"),)))],
        config,
        FAKE_CREDS,
        cast(RunWriter, writer),
        max_workers=2,
    )

    executor = TrackingExecutor.last
    assert executor is not None
    assert executor.peak_outstanding == 4
    assert writer.projects == list(projects)


def test_service_disabled_reason_names_the_api() -> None:
    error = (
        'PermissionDenied: 403 Cloud Scheduler API has not been used in project p1 '
        'before or it is disabled. [reason: "SERVICE_DISABLED" '
        'metadata { key: "service" value: "cloudscheduler.googleapis.com" }]'
    )

    assert service_disabled_reason(error) == "cloudscheduler.googleapis.com not enabled"


def test_service_disabled_reason_ignores_real_permission_errors() -> None:
    error = "PermissionDenied: 403 Permission 'storage.buckets.list' denied on project p1"

    assert service_disabled_reason(error) is None


def test_service_disabled_reason_falls_back_when_service_unnamed() -> None:
    assert service_disabled_reason("HttpError 403: SERVICE_DISABLED") == "API not enabled"


class DeadlineExceeded(Exception):
    pass


class FakeResp:
    def __init__(self, status: int) -> None:
        self.status = status


class FakeHttpError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HttpError {status}")
        self.resp = FakeResp(status)


def test_is_transient_matches_timeout_by_class_name() -> None:
    assert is_transient(DeadlineExceeded("504"))


def test_is_transient_matches_http_status() -> None:
    assert is_transient(FakeHttpError(503))


def test_is_transient_rejects_permission_errors() -> None:
    assert not is_transient(PermissionError("denied"))
    assert not is_transient(FakeHttpError(403))


def test_collect_rows_retries_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gcp_inventory.runner.RETRY_BASE_DELAY", 0.0)
    attempts = {"n": 0}

    def flaky(ctx: CollectorContext) -> RowIterator:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise DeadlineExceeded("504 Deadline Exceeded")
        yield (ctx.project, "ok")

    spec = CollectorSpec(
        name="flaky", filename="f.csv", header=("a", "b"), scope=Scope.GLOBAL, fn=flaky
    )
    rows, error = _collect_rows(spec, CollectorContext("p1", None, FAKE_CREDS))

    assert error is None
    assert rows == [("p1", "ok")]
    assert attempts["n"] == 3


def test_collect_rows_does_not_duplicate_rows_across_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retried collector re-yields what it already yielded."""
    monkeypatch.setattr("gcp_inventory.runner.RETRY_BASE_DELAY", 0.0)
    attempts = {"n": 0}

    def flaky(ctx: CollectorContext) -> RowIterator:
        attempts["n"] += 1
        yield (ctx.project, "first")
        if attempts["n"] < 2:
            raise DeadlineExceeded("504 Deadline Exceeded")
        yield (ctx.project, "second")

    spec = CollectorSpec(
        name="flaky", filename="f.csv", header=("a", "b"), scope=Scope.GLOBAL, fn=flaky
    )
    rows, error = _collect_rows(spec, CollectorContext("p1", None, FAKE_CREDS))

    assert error is None
    assert rows == [("p1", "first"), ("p1", "second")]


def test_collect_rows_does_not_retry_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("gcp_inventory.runner.RETRY_BASE_DELAY", 0.0)
    attempts = {"n": 0}

    def denied(ctx: CollectorContext) -> RowIterator:
        attempts["n"] += 1
        raise PermissionError("denied")
        yield  # pragma: no cover

    spec = CollectorSpec(
        name="denied", filename="d.csv", header=("a", "b"), scope=Scope.GLOBAL, fn=denied
    )
    rows, error = _collect_rows(spec, CollectorContext("p1", None, FAKE_CREDS))

    assert error is not None
    assert rows == []
    assert attempts["n"] == 1


def test_collect_rows_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gcp_inventory.runner.RETRY_BASE_DELAY", 0.0)
    attempts = {"n": 0}

    def always_timing_out(ctx: CollectorContext) -> RowIterator:
        attempts["n"] += 1
        raise DeadlineExceeded("504 Deadline Exceeded")
        yield  # pragma: no cover

    spec = CollectorSpec(
        name="slow",
        filename="s.csv",
        header=("a", "b"),
        scope=Scope.GLOBAL,
        fn=always_timing_out,
    )
    rows, error = _collect_rows(spec, CollectorContext("p1", None, FAKE_CREDS))

    assert error is not None and "DeadlineExceeded" in error
    assert attempts["n"] == MAX_ATTEMPTS


def test_vpc_service_controls_reason_extracts_the_identifier() -> None:
    error = (
        "Forbidden: 403 GET https://bigquery.googleapis.com/...: VPC Service Controls: "
        "Request is prohibited by organization's policy. "
        "vpcServiceControlsUniqueIdentifier: zvW4mQKV6irg5Nm25Div"
    )

    assert vpc_service_controls_reason(error) == (
        "blocked by VPC Service Controls (uniqueIdentifier: zvW4mQKV6irg5Nm25Div)"
    )


def test_vpc_service_controls_reason_without_identifier() -> None:
    assert vpc_service_controls_reason("403 VPC Service Controls") == (
        "blocked by VPC Service Controls"
    )


def test_vpc_service_controls_reason_ignores_other_errors() -> None:
    assert vpc_service_controls_reason("403 Permission 'storage.buckets.list' denied") is None
    assert vpc_service_controls_reason("403 SERVICE_DISABLED") is None
