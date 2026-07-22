"""List service accounts and their project-level IAM role bindings."""

from __future__ import annotations

import csv
import logging
from typing import TYPE_CHECKING

from gcp_inventory.service_accounts import (
    DEFAULT_SA_PATTERNS,
    is_default_service_account,
)

if TYPE_CHECKING:
    from pathlib import Path

    from google.auth.credentials import Credentials

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_SA_PATTERNS",
    "binding_sort_key",
    "get_service_accounts_with_roles",
    "is_default_service_account",
    "save_roles_csv",
]


def get_service_accounts_with_roles(
    source_project: str, credentials: Credentials
) -> dict[str, dict]:
    """Get all service accounts and their project-level role bindings.

    Args:
        source_project: Source GCP project ID.
        credentials: Credentials used to authenticate to the GCP APIs.

    Returns:
        Dictionary mapping service account email to their details and roles.
    """
    from googleapiclient import discovery

    logger.info("Fetching service accounts from project: %s", source_project)

    iam_service = discovery.build("iam", "v1", credentials=credentials)
    crm_service = discovery.build("cloudresourcemanager", "v1", credentials=credentials)

    accounts: list[dict] = []
    request = iam_service.projects().serviceAccounts().list(
        name=f"projects/{source_project}"
    )
    while request is not None:
        response = request.execute()
        accounts.extend(response.get("accounts", []))
        request = iam_service.projects().serviceAccounts().list_next(
            previous_request=request, previous_response=response
        )

    if not accounts:
        logger.info("No service accounts found in project: %s", source_project)
        return {}

    service_accounts: dict[str, dict] = {}

    logger.info("Fetching project IAM policy...")
    # Version 3 so conditional role bindings arrive with their conditions
    # instead of being dropped or rejected.
    project_policy = crm_service.projects().getIamPolicy(
        resource=source_project,
        body={"options": {"requestedPolicyVersion": 3}},
    ).execute()

    for sa in accounts:
        email = sa["email"]
        name = sa.get("displayName", email.split("@")[0])
        sa_name = email.split("@")[0]

        service_accounts[email] = {
            "name": sa_name,
            "display_name": name,
            "email": email,
            "project_roles": [],
        }

        logger.debug("Processing: %s", email)

        for binding in project_policy.get("bindings", []):
            members = binding.get("members", [])
            if f"serviceAccount:{email}" in members:
                service_accounts[email]["project_roles"].append(
                    {"role": binding["role"], "condition": binding.get("condition")}
                )
                logger.debug("  - Role: %s", binding["role"])

    return service_accounts


def binding_sort_key(binding: dict) -> tuple[str, str]:
    """Stable sort key for a role binding: role, then condition title."""
    condition = binding.get("condition") or {}
    return binding["role"], condition.get("title", "")


def save_roles_csv(service_accounts: dict[str, dict], output_file: str | Path) -> None:
    """Save service accounts and roles to a CSV file.

    Args:
        service_accounts: Dictionary of service account details.
        output_file: Output CSV file path.
    """
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Service Account", "Display Name", "Role", "Condition"])

        for email, details in sorted(service_accounts.items()):
            if is_default_service_account(email):
                continue

            if details["project_roles"]:
                for binding in sorted(details["project_roles"], key=binding_sort_key):
                    condition = binding.get("condition") or {}
                    writer.writerow(
                        [
                            email,
                            details["display_name"],
                            binding["role"],
                            condition.get("title", ""),
                        ]
                    )
            else:
                writer.writerow([email, details["display_name"], "No project roles", ""])

    logger.info("Saved to: %s", output_file)
