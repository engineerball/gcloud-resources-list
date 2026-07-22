"""Tests for gcp_inventory.sa."""

import csv

from gcp_inventory.sa.roles import is_default_service_account, save_roles_csv


def rb(role: str, condition: dict | None = None) -> dict:
    """A role binding as stored in project_roles."""
    return {"role": role, "condition": condition}


def test_is_default_service_account_compute_default() -> None:
    assert is_default_service_account("123456-compute@developer.gserviceaccount.com")


def test_is_default_service_account_appspot() -> None:
    assert is_default_service_account("my-project@appspot.gserviceaccount.com")


def test_is_default_service_account_user_sa_is_false() -> None:
    assert not is_default_service_account("my-sa@my-project.iam.gserviceaccount.com")


def test_save_roles_csv(tmp_path) -> None:
    service_accounts = {
        "default@src-proj-compute@developer.gserviceaccount.com": {
            "name": "default",
            "display_name": "Default",
            "email": "default@src-proj-compute@developer.gserviceaccount.com",
            "project_roles": [rb("roles/owner")],
        },
        "user-sa@src-proj.iam.gserviceaccount.com": {
            "name": "user-sa",
            "display_name": "User SA",
            "email": "user-sa@src-proj.iam.gserviceaccount.com",
            "project_roles": [
                rb("roles/viewer"),
                rb("roles/editor", {"title": "only-dev", "expression": "true"}),
            ],
        },
        "no-roles-sa@src-proj.iam.gserviceaccount.com": {
            "name": "no-roles-sa",
            "display_name": "No Roles SA",
            "email": "no-roles-sa@src-proj.iam.gserviceaccount.com",
            "project_roles": [],
        },
    }

    output_file = tmp_path / "roles.csv"
    save_roles_csv(service_accounts, output_file)

    with output_file.open(newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["Service Account", "Display Name", "Role", "Condition"]
    # Default SA is skipped entirely.
    assert not any("compute@developer" in row[0] for row in rows[1:])
    # Roles for user-sa are sorted; conditional binding carries its title.
    sa = "user-sa@src-proj.iam.gserviceaccount.com"
    assert [sa, "User SA", "roles/editor", "only-dev"] in rows
    assert [sa, "User SA", "roles/viewer", ""] in rows
    # SA with no roles gets the placeholder row.
    assert [
        "no-roles-sa@src-proj.iam.gserviceaccount.com",
        "No Roles SA",
        "No project roles",
        "",
    ] in rows
