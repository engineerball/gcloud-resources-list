"""Tests for shared Google Cloud client construction."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest
from google.auth.credentials import Credentials

from gcp_inventory.client_cache import _clear_client_cache, get_grpc_client


class FakeGrpcTransport:
    grpc_channel = object()

    @classmethod
    def create_channel(cls):
        return object()


def test_grpc_client_cache_reuses_one_instance_across_threads() -> None:
    _clear_client_cache()
    credentials = cast(Credentials, object())
    constructed_with: list[Credentials] = []

    class FakeClient:
        @staticmethod
        def get_transport_class():
            return FakeGrpcTransport

        def __init__(self, *, credentials: Credentials) -> None:
            constructed_with.append(credentials)

    with ThreadPoolExecutor(max_workers=8) as pool:
        clients = list(
            pool.map(lambda _: get_grpc_client(FakeClient, credentials), range(40))
        )

    assert all(client is clients[0] for client in clients)
    assert constructed_with == [credentials]


def test_grpc_client_cache_separates_credentials() -> None:
    _clear_client_cache()
    first_credentials = cast(Credentials, object())
    second_credentials = cast(Credentials, object())

    class FakeClient:
        @staticmethod
        def get_transport_class():
            return FakeGrpcTransport

        def __init__(self, *, credentials: Credentials) -> None:
            self.credentials = credentials

    first = get_grpc_client(FakeClient, first_credentials)
    again = get_grpc_client(FakeClient, first_credentials)
    second = get_grpc_client(FakeClient, second_credentials)

    assert first is again
    assert second is not first
    assert first.credentials is first_credentials
    assert second.credentials is second_credentials


def test_grpc_client_cache_rejects_rest_transport() -> None:
    credentials = cast(Credentials, object())

    class FakeRestTransport:
        pass

    class FakeRestClient:
        @staticmethod
        def get_transport_class():
            return FakeRestTransport

        def __init__(self, *, credentials: Credentials) -> None:
            raise AssertionError("REST client must not be constructed by the gRPC cache")

    with pytest.raises(TypeError, match="gRPC transport"):
        get_grpc_client(FakeRestClient, credentials)
