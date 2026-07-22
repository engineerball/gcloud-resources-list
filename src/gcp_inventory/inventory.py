"""End-to-end inventory run: config -> collectors -> CSVs -> summary."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gcp_inventory.auth import get_credentials
from gcp_inventory.config import load_config
from gcp_inventory.registry import select_collectors
from gcp_inventory.runner import DEFAULT_WORKERS, run
from gcp_inventory.writer import RunWriter

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from gcp_inventory.config import Config
    from gcp_inventory.registry import CollectorSpec
    from gcp_inventory.runner import TaskResult

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FATAL = 1
EXIT_PARTIAL = 2


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def run_inventory(
    config_path: str | Path = "config.yaml",
    output_root: str | Path = "output",
    max_workers: int = DEFAULT_WORKERS,
    collector_names: Sequence[str] | None = None,
) -> int:
    """Run the inventory. Returns an exit code: 0 ok, 2 partial failure.

    Fatal problems (bad config, no credentials) raise; the CLI maps them to 1.
    """
    config = load_config(config_path)
    # Resolve the selection before credentials: a mistyped collector name should
    # fail immediately rather than after an ADC round trip.
    specs = select_collectors(collector_names)
    credentials = get_credentials()
    started = datetime.now(UTC)
    run_id = started.strftime("%Y%m%dT%H%M%S") + f"-{started.microsecond:06d}"

    # Name the collectors only when filtered, so a full run's log line is
    # unchanged and a partial one cannot be mistaken for it.
    selection = "" if collector_names is None else f" [{', '.join(s.name for s in specs)}]"
    logger.info(
        "starting %srun %s: %d collectors%s x %d projects (locations: %s)",
        "" if collector_names is None else "filtered ",
        run_id,
        len(specs),
        selection,
        len(config.projects),
        ", ".join(config.locations),
    )

    writer = RunWriter(output_root, run_id)
    try:
        results = run(specs, config, credentials, writer, max_workers=max_workers)
        summary = _summarize(
            run_id,
            started,
            config,
            results,
            specs,
            filtered=collector_names is not None,
        )
        summary_path = writer.staging_dir / "run_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        final_dir = writer.finalize()
    finally:
        writer.close()

    failed = summary["tasks"]["failed"]
    blocked = summary["tasks"]["blocked"]
    logger.info(
        "run %s finished: %d/%d tasks ok, %d skipped, %d blocked, %d failed, output: %s",
        run_id,
        summary["tasks"]["succeeded"],
        summary["tasks"]["total"],
        summary["tasks"]["skipped"],
        blocked,
        failed,
        final_dir,
    )
    for reason, count in sorted(summary["skipped_by_reason"].items()):
        logger.info("skipped %d tasks: %s", count, reason)
    if blocked:
        # Loud and last-ish: these projects are in the CSVs but under-reported,
        # which is more dangerous than a project that failed outright.
        logger.warning(
            "INCOMPLETE: %d tasks blocked by policy across %d project(s): %s",
            blocked,
            len(summary["blocked_projects"]),
            ", ".join(summary["blocked_projects"]),
        )
        for entry in summary["blocked"]:
            logger.warning(
                "blocked: %s project=%s location=%s: %s",
                entry["collector"],
                entry["project"],
                entry["location"] or "-",
                entry["reason"],
            )
    if failed or blocked:
        for failure in summary["failures"]:
            logger.warning(
                "failed: %s project=%s location=%s: %s",
                failure["collector"],
                failure["project"],
                failure["location"] or "-",
                _first_line(failure["error"]),
            )
        return EXIT_PARTIAL
    return EXIT_OK


def _first_line(error: str | None) -> str:
    """Enough of *error* to identify it in a log line.

    Full text stays in run_summary.json; a gRPC error body repeated in the
    terminal is what made real failures unreadable.
    """
    if not error:
        return ""
    first = error.splitlines()[0]
    return first if len(first) <= 200 else first[:197] + "..."


def _summarize(
    run_id: str,
    started: datetime,
    config: Config,
    results: list[TaskResult],
    specs: Sequence[CollectorSpec],
    *,
    filtered: bool,
) -> dict:
    rows_by_collector: dict[str, int] = {}
    for result in results:
        rows_by_collector[result.collector] = (
            rows_by_collector.get(result.collector, 0) + result.rows
        )
    failures = [r for r in results if not r.ok]
    skipped = [r for r in results if r.skipped]
    blocked = [r for r in results if r.blocked]
    skipped_by_reason: dict[str, int] = {}
    for result in skipped:
        reason = result.skipped_reason or "unknown"
        skipped_by_reason[reason] = skipped_by_reason.get(reason, 0) + 1
    return {
        "run_id": run_id,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "projects": list(config.projects),
        "locations": list(config.locations),
        "collectors": {
            "selected": [spec.name for spec in specs],
            "filtered": filtered,
        },
        "tasks": {
            "total": len(results),
            "succeeded": len(results) - len(failures) - len(skipped) - len(blocked),
            "skipped": len(skipped),
            "blocked": len(blocked),
            "failed": len(failures),
        },
        "rows_by_collector": rows_by_collector,
        "skipped_by_reason": skipped_by_reason,
        "blocked": [
            {
                "collector": r.collector,
                "project": r.project,
                "location": r.location,
                "reason": r.blocked_reason,
            }
            for r in blocked
        ],
        "blocked_projects": sorted({r.project for r in blocked}),
        "skipped": [
            {
                "collector": r.collector,
                "project": r.project,
                "location": r.location,
                "reason": r.skipped_reason,
            }
            for r in skipped
        ],
        "failures": [
            {
                "collector": r.collector,
                "project": r.project,
                "location": r.location,
                "error": r.error,
            }
            for r in failures
        ],
    }
