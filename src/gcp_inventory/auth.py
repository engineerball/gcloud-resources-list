"""Application Default Credentials, resolved once per process."""

from __future__ import annotations

from functools import lru_cache
from typing import cast

from google.auth.credentials import Credentials


@lru_cache(maxsize=1)
def get_credentials() -> Credentials:
    """Return cached ADC credentials."""
    from google.auth import default

    credentials, _ = default()
    return cast(Credentials, credentials)
