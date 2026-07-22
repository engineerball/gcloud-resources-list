"""Client construction for collectors: a shared cache for gRPC clients, and
per-task construction for REST clients, which cannot be shared safely."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import TYPE_CHECKING, TypeVar, cast

if TYPE_CHECKING:
    from google.auth.credentials import Credentials

ClientT = TypeVar("ClientT")

_CLIENTS: dict[tuple[object, int], tuple[Credentials, object]] = {}
_CLIENTS_LOCK = Lock()


def new_rest_client(
    client_class: Callable[..., ClientT],
    credentials: Credentials,
    **client_options: object,
) -> ClientT:
    """Build a REST client with the already-resolved credentials."""
    # REST clients own HTTP sessions that are not documented as thread-safe,
    # so each concurrent task needs its own client.
    return client_class(credentials=credentials, **client_options)


def _is_grpc_client_class(client_class: Callable[..., object]) -> bool:
    get_transport_class = getattr(client_class, "get_transport_class", None)
    if not callable(get_transport_class):
        return False

    transport_class = get_transport_class()
    return hasattr(transport_class, "grpc_channel") and callable(
        getattr(transport_class, "create_channel", None)
    )


def get_grpc_client(
    client_class: Callable[..., ClientT], credentials: Credentials
) -> ClientT:
    """Return one generated gRPC client per class and credential object.

    Google documents generated gRPC clients as safe to share across threads.
    Keeping the credential object in the cache also prevents an object id from
    being reused for different credentials during the process lifetime.
    """
    if not _is_grpc_client_class(client_class):
        class_name = getattr(client_class, "__name__", repr(client_class))
        raise TypeError(f"{class_name} does not use a gRPC transport")

    key = (client_class, id(credentials))
    with _CLIENTS_LOCK:
        cached = _CLIENTS.get(key)
        if cached is None:
            client = client_class(credentials=credentials)
            _CLIENTS[key] = (credentials, client)
            return client
        return cast(ClientT, cached[1])


def _clear_client_cache() -> None:
    """Clear cached clients for isolated tests."""
    with _CLIENTS_LOCK:
        _CLIENTS.clear()
