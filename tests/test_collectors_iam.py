"""Tests for gcp_inventory.collectors.iam."""

from typing import cast

from google.auth.credentials import Credentials

from gcp_inventory.collectors.iam import _list_service_accounts, service_accounts
from gcp_inventory.registry import CollectorContext


class FakeRequest:
    """A single page of the IAM serviceAccounts.list response."""

    def __init__(self, page: dict) -> None:
        self.page = page

    def execute(self) -> dict:
        return self.page


class FakeServiceAccounts:
    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages

    def list(self, name: str) -> FakeRequest:  # noqa: ARG002
        return FakeRequest(self.pages[0])

    def list_next(self, previous_request, previous_response) -> FakeRequest | None:  # noqa: ARG002
        index = self.pages.index(previous_response)
        if index + 1 < len(self.pages):
            return FakeRequest(self.pages[index + 1])
        return None


class FakeProjects:
    def __init__(self, pages: list[dict]) -> None:
        self._accounts = FakeServiceAccounts(pages)

    def serviceAccounts(self) -> FakeServiceAccounts:  # noqa: N802
        return self._accounts


class FakeService:
    def __init__(self, pages: list[dict]) -> None:
        self._projects = FakeProjects(pages)

    def projects(self) -> FakeProjects:
        return self._projects


def sa(email: str) -> dict:
    return {"email": email, "name": f"projects/p/serviceAccounts/{email}"}


def test_list_service_accounts_drops_google_created_accounts() -> None:
    service = FakeService(
        [
            {
                "accounts": [
                    sa("123456-compute@developer.gserviceaccount.com"),
                    sa("my-project@appspot.gserviceaccount.com"),
                    sa("service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"),
                    sa("deploy-bot@my-project.iam.gserviceaccount.com"),
                ]
            }
        ]
    )

    accounts = _list_service_accounts(service, "my-project")

    assert [a["email"] for a in accounts] == [
        "deploy-bot@my-project.iam.gserviceaccount.com"
    ]


def test_list_service_accounts_filters_every_page() -> None:
    """Filtering must happen per page, not only on the first one."""
    service = FakeService(
        [
            {"accounts": [sa("first@my-project.iam.gserviceaccount.com")]},
            {
                "accounts": [
                    sa("123456-compute@developer.gserviceaccount.com"),
                    sa("second@my-project.iam.gserviceaccount.com"),
                ]
            },
        ]
    )

    accounts = _list_service_accounts(service, "my-project")

    assert [a["email"] for a in accounts] == [
        "first@my-project.iam.gserviceaccount.com",
        "second@my-project.iam.gserviceaccount.com",
    ]


def test_service_accounts_uses_static_discovery_and_resolved_credentials(
    monkeypatch,
) -> None:
    credentials = cast(Credentials, object())
    calls: list[tuple[str, str, dict]] = []

    def fake_build(service: str, version: str, **kwargs):
        calls.append((service, version, kwargs))
        return FakeService([{}])

    monkeypatch.setattr("googleapiclient.discovery.build", fake_build)

    assert list(service_accounts(CollectorContext("p", None, credentials))) == []
    assert calls == [
        (
            "iam",
            "v1",
            {"credentials": credentials, "static_discovery": True},
        )
    ]
