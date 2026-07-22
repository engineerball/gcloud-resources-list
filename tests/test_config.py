"""Tests for gcp_inventory.config."""

import pytest

from gcp_inventory.config import Config, ConfigError, load_config, parse_config


def test_parse_valid_config() -> None:
    cfg = parse_config(
        {
            "version": 1,
            "gcp_folder_id": "123456",
            "locations": ["asia-southeast1", "us-central1"],
            "projects": ["proj-a", "proj-b"],
        }
    )
    assert cfg == Config(
        projects=("proj-a", "proj-b"),
        locations=("asia-southeast1", "us-central1"),
        gcp_folder_id="123456",
    )


def test_parse_legacy_singular_location() -> None:
    cfg = parse_config({"location": "asia-southeast1", "projects": ["proj-a"]})
    assert cfg.locations == ("asia-southeast1",)


def test_locations_list_wins_over_legacy_location() -> None:
    cfg = parse_config(
        {
            "locations": ["us-central1"],
            "location": "asia-southeast1",
            "projects": ["proj-a"],
        }
    )
    assert cfg.locations == ("us-central1",)


def test_folder_id_optional_and_stringified() -> None:
    cfg = parse_config(
        {
            "location": "asia-southeast1",
            "projects": ["proj-a"],
            "gcp_folder_id": 123456789012,
        }
    )
    assert cfg.gcp_folder_id == "123456789012"
    assert (
        parse_config({"location": "asia-southeast1", "projects": ["proj-a"]}).gcp_folder_id
        is None
    )


@pytest.mark.parametrize(
    "location", ["asia-southeast1", "us-central1", "us", "eu", "global"]
)
def test_parse_accepts_real_locations(location: str) -> None:
    cfg = parse_config({"locations": [location], "projects": ["proj-a"]})
    assert cfg.locations == (location,)


@pytest.mark.parametrize("project", ["proj-a/locations/us", "proj ..", "proj%2fother"])
def test_parse_rejects_project_path_injection(project: str) -> None:
    with pytest.raises(ConfigError, match=repr(project)):
        parse_config({"locations": ["us-central1"], "projects": [project]})


@pytest.mark.parametrize(
    "location", ["us-central1/other", "..", "../../etc", "us central1", "us%2fother"]
)
def test_parse_rejects_location_path_injection(location: str) -> None:
    with pytest.raises(ConfigError, match=repr(location)):
        parse_config({"locations": [location], "projects": ["proj-a"]})


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        "not a mapping",
        {},
        {"projects": ["proj-a"]},  # no locations
        {"location": "asia-southeast1"},  # no projects
        {"location": "asia-southeast1", "projects": []},
        {"location": "asia-southeast1", "projects": "proj-a"},
        {"location": "asia-southeast1", "projects": ["proj-a", ""]},
        {"location": "asia-southeast1", "projects": ["proj-a", 3]},
        {"location": "asia-southeast1", "projects": ["proj-a", "proj-a"]},
        {"locations": [], "projects": ["proj-a"]},
    ],
)
def test_parse_invalid_config_raises(raw: object) -> None:
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_load_config_missing_file(tmp_path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")


def test_load_config_round_trip(tmp_path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("location: asia-southeast1\nprojects:\n  - proj-a\n")
    cfg = load_config(path)
    assert cfg.projects == ("proj-a",)
    assert cfg.locations == ("asia-southeast1",)
