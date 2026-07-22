"""Tests for collector client reuse and credential propagation."""

from __future__ import annotations

from typing import cast

import pytest
from google.auth.credentials import Credentials

from gcp_inventory.client_cache import _clear_client_cache, new_rest_client
from gcp_inventory.collectors import (
    bigquery,
    cloud_functions,
    cloud_run,
    compute,
    dataform,
    kms,
    scheduler,
    workflows,
)
from gcp_inventory.registry import CollectorContext


class EmptyClient:
    def aggregated_list(self, *, request):  # noqa: ARG002
        return ()

    def list_key_rings(self, *, request):  # noqa: ARG002
        return ()

    def list_services(self, *, request):  # noqa: ARG002
        return ()

    def list_functions(self, *, request):  # noqa: ARG002
        return ()

    def list_workflows(self, *, request):  # noqa: ARG002
        return ()

    def list_jobs(self, *, request):  # noqa: ARG002
        return ()

    def list_repositories(self, *, request):  # noqa: ARG002
        return ()

    def list_data_policies(self, *, request):  # noqa: ARG002
        return ()


@pytest.mark.parametrize(
    ("collector_module", "collector_fn"),
    [
        (kms, kms.cloud_kms_keys),
        (cloud_run, cloud_run.cloud_run_services),
        (cloud_functions, cloud_functions.cloud_functions),
        (workflows, workflows.cloud_workflows),
        (scheduler, scheduler.cloud_scheduler),
        (dataform, dataform.cloud_dataform),
        (bigquery, bigquery.cloud_bigquery_datapolicies),
    ],
)
def test_generated_clients_receive_resolved_credentials(
    monkeypatch: pytest.MonkeyPatch, collector_module, collector_fn
) -> None:
    credentials = cast(Credentials, object())
    received: list[Credentials] = []

    def fake_get_client(client_class, passed_credentials):  # noqa: ARG001
        received.append(passed_credentials)
        return EmptyClient()

    monkeypatch.setattr(collector_module, "get_grpc_client", fake_get_client)

    assert list(collector_fn(CollectorContext("p", "r", credentials))) == []
    assert received == [credentials]


def test_compute_rest_client_is_not_reused_across_collector_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.cloud import compute_v1

    _clear_client_cache()
    credentials = cast(Credentials, object())
    constructed_with: list[Credentials] = []

    class FakeComputeClient:
        def __init__(self, *, credentials: Credentials) -> None:
            constructed_with.append(credentials)

        def aggregated_list(self, *, request):  # noqa: ARG002
            return ()

    monkeypatch.setattr(compute_v1, "InstancesClient", FakeComputeClient)

    context = CollectorContext("p", "r", credentials)
    assert list(compute.cloud_compute_engine_instances(context)) == []
    assert list(compute.cloud_compute_engine_instances(context)) == []
    assert constructed_with == [credentials, credentials]


def test_rest_client_receives_options_and_resolved_credentials() -> None:
    credentials = cast(Credentials, object())
    received: list[tuple[str, Credentials]] = []

    class FakeClient:
        def __init__(self, *, project: str, credentials: Credentials) -> None:
            received.append((project, credentials))

    client = new_rest_client(FakeClient, credentials, project="p")

    assert isinstance(client, FakeClient)
    assert received == [("p", credentials)]
