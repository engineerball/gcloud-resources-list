"""Tests for the end-to-end orchestration in gcp_inventory.inventory."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from google.auth.credentials import Credentials

from gcp_inventory import inventory
from gcp_inventory.config import ConfigError
from gcp_inventory.registry import CollectorContext, CollectorSpec, RowIterator, Scope

FAKE_CREDS = cast(Credentials, object())


def good(ctx: CollectorContext) -> RowIterator:
    yield (ctx.project, "ok")


def bad(ctx: CollectorContext) -> RowIterator:
    raise PermissionError("denied")
    yield  # pragma: no cover


def vpc_sc_blocked(ctx: CollectorContext) -> RowIterator:
    raise PermissionError(
        "403 GET https://bigquery.googleapis.com/bigquery/v2/projects/p1/datasets: "
        "VPC Service Controls: Request is prohibited by organization's policy. "
        "vpcServiceControlsUniqueIdentifier: zvW4mQKV6irg"
    )
    yield  # pragma: no cover


def api_disabled(ctx: CollectorContext) -> RowIterator:
    raise PermissionError(
        "403 Workflows API has not been used in project p1 before or it is "
        "disabled. [reason: SERVICE_DISABLED ... workflows.googleapis.com]"
    )
    yield  # pragma: no cover


def make_spec(name: str, fn) -> CollectorSpec:
    return CollectorSpec(
        name=name,
        filename=f"{name}.csv",
        header=("Project", "Value"),
        scope=Scope.GLOBAL,
        fn=fn,
    )


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Config file + fakes for credentials; returns (config_path, output_root)."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("location: us-central1\nprojects:\n  - proj-a\n")
    monkeypatch.setattr(inventory, "get_credentials", lambda: FAKE_CREDS)
    return config_path, tmp_path / "output"


def test_run_inventory_success(env, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path, output_root = env
    monkeypatch.setattr(
        inventory, "select_collectors", lambda _names: (make_spec("a", good), make_spec("b", good))
    )

    code = inventory.run_inventory(config_path, output_root)

    assert code == inventory.EXIT_OK
    run_dir = (output_root / "latest").resolve()
    assert (run_dir / "a.csv").read_text().splitlines() == ["Project,Value", "proj-a,ok"]
    assert (run_dir / "b.csv").read_text().splitlines() == ["Project,Value", "proj-a,ok"]
    summary = json.loads((run_dir / "run_summary.json").read_text())
    assert summary["tasks"] == {"total": 2, "succeeded": 2, "skipped": 0, "blocked": 0, "failed": 0}
    assert summary["rows_by_collector"] == {"a": 1, "b": 1}
    assert summary["collectors"] == {"selected": ["a", "b"], "filtered": False}
    assert summary["failures"] == []


def test_run_inventory_uses_selected_collectors_and_records_selection(
    env, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, output_root = env
    available = (make_spec("a", good), make_spec("b", good))

    def fake_select(names):
        assert names == ["b"]
        return (available[1],)

    monkeypatch.setattr(inventory, "select_collectors", fake_select)

    code = inventory.run_inventory(config_path, output_root, collector_names=["b"])

    assert code == inventory.EXIT_OK
    run_dir = (output_root / "latest").resolve()
    assert not (run_dir / "a.csv").exists()
    assert (run_dir / "b.csv").exists()
    summary = json.loads((run_dir / "run_summary.json").read_text())
    assert summary["collectors"] == {"selected": ["b"], "filtered": True}


def test_run_inventory_partial_failure(env, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path, output_root = env
    monkeypatch.setattr(
        inventory, "select_collectors", lambda _names: (make_spec("a", good), make_spec("b", bad))
    )

    code = inventory.run_inventory(config_path, output_root)

    assert code == inventory.EXIT_PARTIAL
    summary = json.loads((output_root / "latest" / "run_summary.json").read_text())
    assert summary["tasks"] == {"total": 2, "succeeded": 1, "skipped": 0, "blocked": 0, "failed": 1}
    assert summary["failures"][0]["collector"] == "b"
    assert "denied" in summary["failures"][0]["error"]


def test_run_inventory_missing_config_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        inventory.run_inventory(tmp_path / "nope.yaml", tmp_path / "output")


def test_run_inventory_skips_disabled_apis(env, monkeypatch: pytest.MonkeyPatch) -> None:
    """A project that never enabled an API has nothing to inventory, so the
    run must stay green instead of reporting a failure."""
    config_path, output_root = env
    monkeypatch.setattr(
        inventory,
        "select_collectors",
        lambda _names: (make_spec("a", good), make_spec("b", api_disabled)),
    )

    code = inventory.run_inventory(config_path, output_root)

    assert code == inventory.EXIT_OK
    summary = json.loads((output_root / "latest" / "run_summary.json").read_text())
    assert summary["tasks"] == {"total": 2, "succeeded": 1, "skipped": 1, "blocked": 0, "failed": 0}
    assert summary["failures"] == []
    assert summary["skipped"][0]["collector"] == "b"
    assert summary["skipped_by_reason"] == {"workflows.googleapis.com not enabled": 1}


def test_run_inventory_keeps_real_failures_separate(
    env, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, output_root = env
    monkeypatch.setattr(
        inventory,
        "select_collectors",
        lambda _names: (make_spec("a", api_disabled), make_spec("b", bad)),
    )

    code = inventory.run_inventory(config_path, output_root)

    assert code == inventory.EXIT_PARTIAL
    summary = json.loads((output_root / "latest" / "run_summary.json").read_text())
    assert summary["tasks"] == {"total": 2, "succeeded": 0, "skipped": 1, "blocked": 0, "failed": 1}
    assert [f["collector"] for f in summary["failures"]] == ["b"]


def test_run_inventory_reports_vpc_sc_as_blocked(env, monkeypatch: pytest.MonkeyPatch) -> None:
    """A perimeter denial means the data exists and was not read, so the run
    must not look clean."""
    config_path, output_root = env
    monkeypatch.setattr(
        inventory,
        "select_collectors",
        lambda _names: (make_spec("a", good), make_spec("b", vpc_sc_blocked)),
    )

    code = inventory.run_inventory(config_path, output_root)

    assert code == inventory.EXIT_PARTIAL
    summary = json.loads((output_root / "latest" / "run_summary.json").read_text())
    assert summary["tasks"] == {
        "total": 2,
        "succeeded": 1,
        "skipped": 0,
        "blocked": 1,
        "failed": 0,
    }
    assert summary["failures"] == []
    assert summary["blocked_projects"] == ["proj-a"]
    assert "zvW4mQKV6irg" in summary["blocked"][0]["reason"]
