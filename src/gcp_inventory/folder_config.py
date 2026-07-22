"""Generate config.yaml from the ACTIVE projects under a GCP folder."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from gcp_inventory.config import ConfigError

logger = logging.getLogger(__name__)

DEFAULT_LOCATIONS = ("asia-southeast1",)

_REAUTH_HINT = (
    "If the credentials are simply stale, refresh them with "
    "'gcloud auth application-default login'; a 403 looks identical "
    "whether the role is missing or the token has expired."
)


def explain_api_error(exc: Exception, folder_id: str, permission: str, role: str) -> str:
    """A remediation message for a Resource Manager call that was refused.

    The raw gRPC error is a 40-line traceback that names the permission but
    not the role that grants it, and gives no hint that expired credentials
    produce the same 403.
    """
    from google.api_core import exceptions as api_exceptions

    if isinstance(exc, api_exceptions.Unauthenticated):
        return (
            f"Not authenticated to read folder {folder_id}. "
            "Run 'gcloud auth application-default login'."
        )

    if isinstance(exc, api_exceptions.PermissionDenied):
        return (
            f"Permission '{permission}' denied on folder {folder_id}. "
            f"Grant '{role}' on that folder, or check the folder ID exists. "
            f"{_REAUTH_HINT}"
        )

    return f"Failed to read folder {folder_id}: {exc}"


def _folder_id_from_name(name: str) -> str:
    """``folders/123`` -> ``123``."""
    return name.rsplit("/", 1)[-1]


def collect_folder_ids(list_subfolders, root_folder_id: str) -> list[str]:
    """*root_folder_id* plus every folder nested beneath it, breadth first.

    Resource Manager has no "search projects under this subtree" query, only
    "directly under this parent", so the subtree has to be walked explicitly.

    *list_subfolders* takes a folder ID and returns the IDs of its ACTIVE
    direct children.
    """
    ordered: list[str] = []
    seen: set[str] = set()
    queue: list[str] = [root_folder_id]

    while queue:
        folder_id = queue.pop(0)
        if folder_id in seen:
            continue
        seen.add(folder_id)
        ordered.append(folder_id)
        queue.extend(list_subfolders(folder_id))

    return ordered


def _list_active_subfolder_ids(client, folder_id: str) -> list[str]:
    """IDs of ACTIVE folders directly under *folder_id*, following pagination."""
    from google.cloud import resourcemanager_v3

    request = resourcemanager_v3.ListFoldersRequest(parent=f"folders/{folder_id}")
    try:
        folders = list(client.list_folders(request=request))
    except Exception as exc:
        raise ConfigError(
            explain_api_error(
                exc,
                folder_id,
                "resourcemanager.folders.list",
                "roles/resourcemanager.folderViewer",
            )
        ) from exc

    return [
        _folder_id_from_name(folder.name)
        for folder in folders
        if folder.state == resourcemanager_v3.Folder.State.ACTIVE
    ]


def _list_direct_active_projects(client, folder_id: str) -> list[str]:
    """Project IDs of ACTIVE projects directly under *folder_id*."""
    from google.cloud import resourcemanager_v3

    request = resourcemanager_v3.SearchProjectsRequest(
        query=f"parent.type:folder parent.id:{folder_id}"
    )
    try:
        projects = list(client.search_projects(request=request))
    except Exception as exc:
        raise ConfigError(
            explain_api_error(
                exc,
                folder_id,
                "resourcemanager.projects.list",
                "roles/browser",
            )
        ) from exc

    return [
        project.project_id
        for project in projects
        if project.state == resourcemanager_v3.Project.State.ACTIVE
    ]


def list_active_projects(folder_id: str) -> list[str]:
    """Project IDs of ACTIVE projects under *folder_id*, including sub-folders."""
    from google.cloud import resourcemanager_v3

    folders_client = resourcemanager_v3.FoldersClient()
    projects_client = resourcemanager_v3.ProjectsClient()

    folder_ids = collect_folder_ids(
        lambda fid: _list_active_subfolder_ids(folders_client, fid), folder_id
    )
    logger.info("walking %d folders under %s", len(folder_ids), folder_id)

    # A project has exactly one parent, so the same ID cannot come back twice;
    # dedupe anyway so a surprising API response cannot duplicate rows.
    project_ids: list[str] = []
    seen: set[str] = set()
    for fid in folder_ids:
        for project_id in _list_direct_active_projects(projects_client, fid):
            if project_id not in seen:
                seen.add(project_id)
                project_ids.append(project_id)

    return project_ids


def generate_config(folder_id: str, locations: tuple[str, ...] = DEFAULT_LOCATIONS) -> dict:
    """Build a config mapping for the projects under *folder_id*."""
    projects = list_active_projects(folder_id)
    if not projects:
        raise ConfigError(f"No ACTIVE projects found under folder {folder_id}.")
    logger.info("found %d active projects under folder %s", len(projects), folder_id)
    return {
        "version": 1,
        "gcp_folder_id": folder_id,
        "locations": list(locations),
        "projects": projects,
    }


def write_config(config: dict, path: str | Path = "config.yaml") -> Path:
    """Write *config* as YAML to *path* (overwrites)."""
    path = Path(path)
    path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
    logger.info("wrote %s", path)
    return path
