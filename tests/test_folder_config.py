"""Tests for gcp_inventory.folder_config."""

import pytest
from google.api_core import exceptions as api_exceptions

from gcp_inventory.config import ConfigError
from gcp_inventory.folder_config import (
    _list_active_subfolder_ids,
    collect_folder_ids,
    explain_api_error,
    generate_config,
)


def tree_walker(tree: dict[str, list[str]]):
    """A list_subfolders callable backed by an in-memory folder tree."""
    return lambda folder_id: tree.get(folder_id, [])


def test_collect_folder_ids_returns_root_when_no_subfolders() -> None:
    assert collect_folder_ids(tree_walker({}), "100") == ["100"]


def test_collect_folder_ids_walks_nested_folders() -> None:
    tree = {
        "100": ["200", "300"],
        "200": ["210"],
        "210": ["211"],
    }

    assert collect_folder_ids(tree_walker(tree), "100") == [
        "100",
        "200",
        "300",
        "210",
        "211",
    ]


def test_collect_folder_ids_visits_each_folder_once() -> None:
    """A repeated child must not send the walk into an infinite loop."""
    tree = {"100": ["200", "200"], "200": ["100"]}

    assert collect_folder_ids(tree_walker(tree), "100") == ["100", "200"]


def test_generate_config_includes_subfolder_projects(monkeypatch) -> None:
    monkeypatch.setattr(
        "gcp_inventory.folder_config.list_active_projects",
        lambda folder_id: ["root-proj", "nested-proj"],  # noqa: ARG005
    )

    config = generate_config("100", locations=("asia-southeast1",))

    assert config["projects"] == ["root-proj", "nested-proj"]
    assert config["gcp_folder_id"] == "100"
    assert config["locations"] == ["asia-southeast1"]


def test_explain_api_error_names_the_role_for_permission_denied() -> None:
    message = explain_api_error(
        api_exceptions.PermissionDenied("nope"),
        "554481406041",
        "resourcemanager.folders.list",
        "roles/resourcemanager.folderViewer",
    )

    assert "roles/resourcemanager.folderViewer" in message
    assert "554481406041" in message
    # A 403 is ambiguous between a missing role and an expired token, so the
    # message has to mention re-auth as well.
    assert "application-default login" in message


def test_explain_api_error_tells_unauthenticated_callers_to_log_in() -> None:
    message = explain_api_error(
        api_exceptions.Unauthenticated("nope"),
        "554481406041",
        "resourcemanager.folders.list",
        "roles/resourcemanager.folderViewer",
    )

    assert "application-default login" in message


def test_explain_api_error_passes_through_unexpected_errors() -> None:
    message = explain_api_error(
        RuntimeError("kaboom"),
        "554481406041",
        "resourcemanager.folders.list",
        "roles/resourcemanager.folderViewer",
    )

    assert "kaboom" in message


class RefusingFoldersClient:
    def list_folders(self, request):  # noqa: ARG002
        raise api_exceptions.PermissionDenied("denied")


def test_list_subfolders_raises_config_error_on_permission_denied() -> None:
    """The CLI only renders ConfigError cleanly; anything else is a traceback."""
    with pytest.raises(ConfigError, match="roles/resourcemanager.folderViewer"):
        _list_active_subfolder_ids(RefusingFoldersClient(), "554481406041")


def test_generate_config_rejects_empty_folder(monkeypatch) -> None:
    monkeypatch.setattr(
        "gcp_inventory.folder_config.list_active_projects",
        lambda folder_id: [],  # noqa: ARG005
    )

    with pytest.raises(ConfigError):
        generate_config("100")
