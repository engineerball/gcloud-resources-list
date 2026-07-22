"""IAM service accounts and their role bindings."""

from __future__ import annotations

import logging

from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector
from gcp_inventory.service_accounts import is_default_service_account

_LOGGER = logging.getLogger(__name__)


@collector(
    name="service_accounts",
    filename="service_accounts.csv",
    header=("Project", "Email"),
    scope=Scope.GLOBAL,
)
def service_accounts(ctx: CollectorContext) -> RowIterator:
    from googleapiclient import discovery

    service = discovery.build(
        "iam", "v1", credentials=ctx.credentials, static_discovery=True
    )
    for account in _list_service_accounts(service, ctx.project):
        yield ctx.project, account["email"]


def _list_service_accounts(service, project: str) -> list[dict]:
    """User-managed service accounts in *project*, following pagination.

    Google-created accounts (the Compute Engine and App Engine defaults, and
    any service agent) are dropped: the inventory is about what humans created.

    The IAM API pages at 20 accounts by default; a single .execute() call
    silently truncates anything beyond the first page.
    """
    accounts: list[dict] = []
    request = service.projects().serviceAccounts().list(name=f"projects/{project}")
    while request is not None:
        response = request.execute()
        accounts.extend(
            account
            for account in response.get("accounts", [])
            if not is_default_service_account(account["email"])
        )
        request = service.projects().serviceAccounts().list_next(
            previous_request=request, previous_response=response
        )
    return accounts


def _find_organization_id(resource_manager, project: str) -> str | None:
    """Walk the project's ancestry to the owning organization, if any.

    Checking only the immediate parent misses every project that lives
    under a folder.
    """
    ancestry = resource_manager.projects().getAncestry(
        projectId=project, body={}
    ).execute()
    for ancestor in ancestry.get("ancestor", []):
        resource_id = ancestor.get("resourceId", {})
        if resource_id.get("type") == "organization":
            return resource_id["id"]
    return None


@collector(
    name="service_account_roles",
    filename="service_account_roles.csv",
    header=("Project", "Service Account", "Role", "Binding Type"),
    scope=Scope.GLOBAL,
)
def service_account_roles(ctx: CollectorContext) -> RowIterator:
    from googleapiclient import discovery

    service = discovery.build(
        "iam", "v1", credentials=ctx.credentials, static_discovery=True
    )
    resource_manager = discovery.build(
        "cloudresourcemanager",
        "v1",
        credentials=ctx.credentials,
        static_discovery=True,
    )

    accounts = _list_service_accounts(service, ctx.project)
    if not accounts:
        return

    project_policy = resource_manager.projects().getIamPolicy(
        resource=ctx.project, body={}
    ).execute()

    org_policy: dict | None = None
    try:
        org_id = _find_organization_id(resource_manager, ctx.project)
        if org_id is not None:
            org_policy = (
                resource_manager.organizations()
                .getIamPolicy(resource=f"organizations/{org_id}", body={})
                .execute()
            )
    except Exception as exc:  # noqa: BLE001
        _LOGGER.warning("Failed to fetch organization IAM policy: %s", exc)

    for account in accounts:
        email = account["email"]

        try:
            direct_policy = (
                service.projects()
                .serviceAccounts()
                .getIamPolicy(resource=account["name"])
                .execute()
            )
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "Failed to fetch direct IAM policy for %s: %s", email, exc
            )
            direct_policy = {}

        for binding in direct_policy.get("bindings", []):
            yield ctx.project, email, binding["role"], "Direct"

        for binding in project_policy.get("bindings", []):
            members = binding.get("members", [])
            if f"serviceAccount:{email}" in members:
                yield ctx.project, email, binding["role"], "Project"

        if org_policy is not None:
            for binding in org_policy.get("bindings", []):
                members = binding.get("members", [])
                if f"serviceAccount:{email}" in members:
                    yield ctx.project, email, binding["role"], "Organization"
