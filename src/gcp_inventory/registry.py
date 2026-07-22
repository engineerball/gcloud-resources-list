"""Collector registry: specs, contexts, and the @collector decorator."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.auth.credentials import Credentials


class Scope(Enum):
    """How a collector fans out over the configuration."""

    GLOBAL = "global"  # one task per project
    REGIONAL = "regional"  # one task per (project, location)


@dataclass(frozen=True)
class CollectorContext:
    """Everything a collector needs for a single task."""

    project: str
    location: str | None
    credentials: Credentials

    @property
    def parent(self) -> str:
        """The ``projects/{project}/locations/{location}`` resource parent."""
        if self.location is None:
            raise RuntimeError("parent requires a location; collector scope is GLOBAL")
        return f"projects/{self.project}/locations/{self.location}"


RowIterator = Iterator[Sequence[object]]
CollectorFn = Callable[[CollectorContext], RowIterator]


@dataclass(frozen=True)
class CollectorSpec:
    """A registered collector and the CSV it produces."""

    name: str
    filename: str
    header: tuple[str, ...]
    scope: Scope
    fn: CollectorFn


_REGISTRY: dict[str, CollectorSpec] = {}


class CollectorSelectionError(ValueError):
    """Raised when a requested collector name is not registered."""


def collector(
    name: str, filename: str, header: Sequence[str], scope: Scope
) -> Callable[[CollectorFn], CollectorFn]:
    """Register a collector function.

    The decorated function receives a :class:`CollectorContext` and yields CSV
    rows. The runner owns iteration over projects/locations, error isolation,
    and writing; the collector owns only the API call and row extraction.
    """

    def decorate(fn: CollectorFn) -> CollectorFn:
        if name in _REGISTRY:
            raise ValueError(f"Duplicate collector name: {name}")
        _REGISTRY[name] = CollectorSpec(
            name=name, filename=filename, header=tuple(header), scope=scope, fn=fn
        )
        return fn

    return decorate


def all_collectors() -> tuple[CollectorSpec, ...]:
    """All registered collectors, in registration order."""
    # Importing the package triggers registration of every collector module.
    import gcp_inventory.collectors  # noqa: F401

    return tuple(_REGISTRY.values())


def select_collectors(names: Sequence[str] | None) -> tuple[CollectorSpec, ...]:
    """Select registered collectors by name, preserving registration order."""
    specs = all_collectors()
    if names is None:
        return specs
    # An empty selection would otherwise publish an output directory with no
    # CSVs at all, which reads as "nothing exists" rather than "nothing ran".
    if not names:
        raise CollectorSelectionError("no collector names given")

    requested = set(names)
    valid_names = [spec.name for spec in specs]
    unknown = sorted(requested.difference(valid_names))
    if unknown:
        unknown_text = ", ".join(unknown)
        valid_text = ", ".join(valid_names)
        raise CollectorSelectionError(
            f"unknown collector name(s): {unknown_text}. Valid collectors: {valid_text}"
        )

    return tuple(spec for spec in specs if spec.name in requested)
