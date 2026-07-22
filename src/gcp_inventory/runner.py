"""Execution harness: fans collectors out over projects/locations,
isolates failures, and reports per-task results."""

from __future__ import annotations

import logging
import random
import re
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

from gcp_inventory.registry import CollectorContext, CollectorSpec, Scope

if TYPE_CHECKING:
    from collections.abc import Sequence

    from google.auth.credentials import Credentials

    from gcp_inventory.config import Config
    from gcp_inventory.writer import RunWriter

logger = logging.getLogger(__name__)

# Sixteen hides independent API latency without creating the quota burst that
# much larger I/O pool defaults can cause against a single project.
DEFAULT_WORKERS = 16
_IN_FLIGHT_PER_WORKER = 2

# A timed-out task is lost from every run until something retries it.
MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0

# Matched on the exception class name so neither client library has to be
# imported here; collectors raise google.api_core exceptions and
# googleapiclient.errors.HttpError interchangeably.
_TRANSIENT_TYPES = frozenset(
    {
        "DeadlineExceeded",
        "InternalServerError",
        "RetryError",
        "ServiceUnavailable",
        "TooManyRequests",
        "TransportError",
    }
)
_TRANSIENT_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})


def is_transient(exc: Exception) -> bool:
    """Whether *exc* is worth another attempt.

    Permission and configuration errors are not: retrying them only slows the
    run down and produces the same answer.
    """
    if type(exc).__name__ in _TRANSIENT_TYPES:
        return True
    # googleapiclient.errors.HttpError carries the status on .resp.
    status = getattr(getattr(exc, "resp", None), "status", None)
    return status in _TRANSIENT_HTTP_STATUS


# A disabled API answers 403, exactly like a real permission problem. Both
# client libraries say so in the message body rather than the status code:
# google.api_core raises PermissionDenied with a SERVICE_DISABLED reason,
# googleapiclient raises HttpError with the same prose.
_SERVICE_DISABLED_MARKERS = (
    "SERVICE_DISABLED",
    "has not been used in project",
    "it is disabled",
)
_SERVICE_NAME = re.compile(r"([a-z0-9-]+\.googleapis\.com)")


def service_disabled_reason(error: str) -> str | None:
    """The API that is switched off, if *error* says one is; else None.

    A project that never enabled an API is not a failure to report - there is
    simply nothing of that resource type to inventory.
    """
    if not any(marker in error for marker in _SERVICE_DISABLED_MARKERS):
        return None
    match = _SERVICE_NAME.search(error)
    return f"{match.group(1)} not enabled" if match else "API not enabled"


# VPC Service Controls also answers 403. Unlike a disabled API this means the
# resources exist and could not be read, so it must never be treated as "there
# was nothing here".
_VPC_SC_MARKERS = (
    "VPC Service Controls",
    "vpcServiceControls",
    "Request is prohibited by organization's policy",
)
# The identifier an administrator needs to find the denial in the audit logs.
_VPC_SC_ID = re.compile(r"vpcServiceControlsUniqueIdentifier: ([A-Za-z0-9_-]+)")


def vpc_service_controls_reason(error: str) -> str | None:
    """A description of the perimeter denial in *error*, if it is one."""
    if not any(marker in error for marker in _VPC_SC_MARKERS):
        return None
    match = _VPC_SC_ID.search(error)
    if match is None:
        return "blocked by VPC Service Controls"
    return f"blocked by VPC Service Controls (uniqueIdentifier: {match.group(1)})"


@dataclass(frozen=True)
class TaskResult:
    """Outcome of one (collector, project[, location]) task."""

    collector: str
    project: str
    location: str | None
    rows: int = 0
    error: str | None = None
    skipped_reason: str | None = None
    blocked_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def skipped(self) -> bool:
        return self.skipped_reason is not None

    @property
    def blocked(self) -> bool:
        return self.blocked_reason is not None


def build_tasks(
    specs: Sequence[CollectorSpec], config: Config, credentials: Credentials
) -> list[tuple[CollectorSpec, CollectorContext]]:
    """Expand collectors × projects (× locations for REGIONAL scope)."""
    tasks: list[tuple[CollectorSpec, CollectorContext]] = []
    for spec in specs:
        for project in config.projects:
            if spec.scope is Scope.GLOBAL:
                tasks.append((spec, CollectorContext(project, None, credentials)))
            else:
                for location in config.locations:
                    tasks.append((spec, CollectorContext(project, location, credentials)))
    return tasks


def run(
    specs: Sequence[CollectorSpec],
    config: Config,
    credentials: Credentials,
    writer: RunWriter,
    max_workers: int = DEFAULT_WORKERS,
) -> list[TaskResult]:
    """Run every task; a failing task never aborts the run.

    API calls run on a bounded thread pool; only this (the calling) thread
    writes CSVs, in task order, so output is deterministic and lock-free.
    """
    tasks = build_tasks(specs, config, credentials)
    results: list[TaskResult] = []
    worker_count = max(1, max_workers)
    task_iter = iter(tasks)
    pending = deque()
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        for _ in range(worker_count * _IN_FLIGHT_PER_WORKER):
            try:
                spec, ctx = next(task_iter)
            except StopIteration:
                break
            pending.append((spec, ctx, pool.submit(_collect_rows, spec, ctx)))

        # A small queue keeps workers fed without retaining every later result
        # behind a slow task at the head of the deterministic write order.
        while pending:
            spec, ctx, future = pending.popleft()
            rows, error = future.result()
            # Unconditional: even an empty or failed task creates its CSV with
            # header, so every registered output file always exists.
            writer.write(spec, rows)
            results.append(_to_result(spec, ctx, len(rows), error))
            try:
                next_spec, next_ctx = next(task_iter)
            except StopIteration:
                continue
            pending.append(
                (
                    next_spec,
                    next_ctx,
                    pool.submit(_collect_rows, next_spec, next_ctx),
                )
            )
    return results


def _collect_rows(
    spec: CollectorSpec, ctx: CollectorContext
) -> tuple[list[Sequence[object]], str | None]:
    """Worker: materialize a collector's rows, retrying transient failures.

    Partial rows survive a failure. Each attempt starts from an empty list:
    re-running a collector re-yields everything it already yielded, so
    accumulating across attempts would duplicate rows.
    """
    rows: list[Sequence[object]] = []
    error: str | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        rows = []
        try:
            for row in spec.fn(ctx):
                rows.append(row)
        except Exception as exc:  # noqa: BLE001 - isolation boundary by design
            error = f"{type(exc).__name__}: {exc}"
            if attempt == MAX_ATTEMPTS or not is_transient(exc):
                return rows, error
            delay = _retry_delay(attempt)
            logger.info(
                "retrying collector=%s project=%s in %.1fs (attempt %d/%d): %s",
                spec.name,
                ctx.project,
                delay,
                attempt + 1,
                MAX_ATTEMPTS,
                type(exc).__name__,
            )
            # Scheduling delayed retries outside the worker would split retry
            # state from partial-row isolation and risk changing task behavior.
            time.sleep(delay)
        else:
            return rows, None

    return rows, error


def _retry_delay(attempt: int) -> float:
    """Exponential backoff, jittered so parallel tasks do not retry in lockstep."""
    delay = RETRY_BASE_DELAY * 2 ** (attempt - 1)
    return delay + random.uniform(0, delay / 4)  # noqa: S311 - backoff, not crypto


def _to_result(
    spec: CollectorSpec, ctx: CollectorContext, row_count: int, error: str | None
) -> TaskResult:
    where = f"collector={spec.name} project={ctx.project}" + (
        f" location={ctx.location}" if ctx.location else ""
    )
    if error is not None:
        reason = service_disabled_reason(error)
        if reason is not None:
            # One line, not the ~25-line gRPC error body: an unused API is
            # routine, and burying it in noise hides the real failures.
            logger.info("skipped %s: %s", where, reason)
            return TaskResult(
                spec.name, ctx.project, ctx.location, rows=row_count, skipped_reason=reason
            )
        blocked = vpc_service_controls_reason(error)
        if blocked is not None:
            # Not a failure to retry and not an absence of data: the resources
            # are there and this run cannot see them. Say so explicitly, or the
            # CSV silently under-reports the project.
            logger.warning("blocked %s: %s", where, blocked)
            return TaskResult(
                spec.name, ctx.project, ctx.location, rows=row_count, blocked_reason=blocked
            )
        logger.warning("failed %s: %s", where, error)
        return TaskResult(spec.name, ctx.project, ctx.location, rows=row_count, error=error)
    logger.info("done %s rows=%d", where, row_count)
    return TaskResult(spec.name, ctx.project, ctx.location, rows=row_count)
