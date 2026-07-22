"""Tests for the CLI argument surface and dispatch."""

from __future__ import annotations

import logging

import pytest

from gcp_inventory import cli


def test_run_defaults() -> None:
    args = cli.build_parser().parse_args(["run"])
    assert (args.command, args.config, args.output) == ("run", "config.yaml", "output")
    assert args.workers > 0
    assert args.collectors is None
    assert args.list_collectors is False


def test_run_repeatable_collectors() -> None:
    args = cli.build_parser().parse_args(
        ["run", "--collector", "service_accounts", "--collector", "cloud_storage_buckets"]
    )
    assert args.collectors == ["service_accounts", "cloud_storage_buckets"]


def test_verbose_works_before_and_after_subcommand() -> None:
    assert getattr(cli.build_parser().parse_args(["-v", "run"]), "verbose", False) is True
    assert getattr(cli.build_parser().parse_args(["run", "-v"]), "verbose", False) is True
    assert getattr(cli.build_parser().parse_args(["run"]), "verbose", False) is False


def test_config_requires_folder_id() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["config"])


def test_config_repeatable_locations() -> None:
    args = cli.build_parser().parse_args(
        ["config", "--folder-id", "123", "--location", "r1", "--location", "r2"]
    )
    assert args.locations == ["r1", "r2"]


def test_main_dispatches_run_and_exits_with_code(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def fake_run_inventory(config, output, max_workers, collector_names):
        seen.update(
            config=config,
            output=output,
            workers=max_workers,
            collector_names=collector_names,
        )
        return 2

    monkeypatch.setattr(cli, "run_inventory", fake_run_inventory)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["run", "--config", "c.yaml", "--workers", "3"])
    assert excinfo.value.code == 2
    assert seen == {
        "config": "c.yaml",
        "output": "output",
        "workers": 3,
        "collector_names": None,
    }


def test_main_maps_config_error_to_exit_1(monkeypatch: pytest.MonkeyPatch) -> None:
    from gcp_inventory.config import ConfigError

    def boom(*a, **k):
        raise ConfigError("bad config")

    monkeypatch.setattr(cli, "run_inventory", boom)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["run"])
    assert excinfo.value.code == 1


def test_list_collectors_exits_without_running_inventory(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "must-not-exist"

    def must_not_run(*args, **kwargs):
        pytest.fail("inventory must not run when listing collectors")

    monkeypatch.setattr(cli, "run_inventory", must_not_run)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["run", "--list-collectors", "--output", str(output)])

    assert excinfo.value.code == 0
    assert not output.exists()
    printed = capsys.readouterr().out
    assert "service_accounts\tGLOBAL" in printed
    assert "cloud_functions\tREGIONAL" in printed


def test_unknown_collector_exits_fatal_and_lists_valid_names(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("location: us-central1\nprojects:\n  - proj-a\n")
    output = tmp_path / "must-not-exist"

    def must_not_authenticate():
        pytest.fail("unknown collector names must fail before authentication")

    monkeypatch.setattr("gcp_inventory.inventory.get_credentials", must_not_authenticate)
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as excinfo:
        cli.main(
            [
                "run",
                "--config",
                str(config_path),
                "--output",
                str(output),
                "--collector",
                "typo",
            ]
        )

    assert excinfo.value.code == 1
    assert not output.exists()
    assert "typo" in caplog.text
    assert "service_accounts" in caplog.text
    assert "cloud_storage_buckets" in caplog.text
