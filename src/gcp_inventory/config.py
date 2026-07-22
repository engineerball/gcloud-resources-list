"""Load and validate the inventory configuration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]")
_LOCATION = re.compile(r"(?:[a-z]+(?:-[a-z0-9]+)+|us|eu|global)")


class ConfigError(ValueError):
    """Raised when the configuration file is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """Validated inventory configuration."""

    projects: tuple[str, ...]
    locations: tuple[str, ...]
    gcp_folder_id: str | None = None


def load_config(path: str | Path = "config.yaml") -> Config:
    """Load and validate the config file at *path*."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(
            f"Config file not found: {path}. "
            "Generate one with 'gcp-inventory config --folder-id <FOLDER_ID>'."
        )
    with path.open() as fh:
        raw = yaml.safe_load(fh)
    return parse_config(raw)


def parse_config(raw: object) -> Config:
    """Validate raw YAML data and build a :class:`Config`."""
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a mapping.")

    projects = _validated_string_list(raw.get("projects"), "projects", _PROJECT_ID)

    # Accept the legacy singular `location: <str>` alongside `locations: [<str>, ...]`.
    raw_locations = raw.get("locations", raw.get("location"))
    if isinstance(raw_locations, str):
        raw_locations = [raw_locations]
    locations = _validated_string_list(raw_locations, "locations", _LOCATION)

    folder_id = raw.get("gcp_folder_id")
    if folder_id is not None:
        folder_id = str(folder_id)

    return Config(projects=projects, locations=locations, gcp_folder_id=folder_id)


def _string_list(value: object, key: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"'{key}' must be a non-empty list.")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"'{key}' entries must be non-empty strings, got: {item!r}")
        items.append(item.strip())
    if len(set(items)) != len(items):
        raise ConfigError(f"'{key}' contains duplicates.")
    return tuple(items)


def _validated_string_list(
    value: object, key: str, pattern: re.Pattern[str]
) -> tuple[str, ...]:
    items = _string_list(value, key)
    for item in items:
        if pattern.fullmatch(item) is None:
            raise ConfigError(f"Invalid '{key}' value: {item!r}")
    return items
