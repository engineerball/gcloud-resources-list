"""Shared helpers for classifying service accounts.

The IAM ``serviceAccounts.list`` API only ever returns user-managed accounts
plus the Compute Engine and App Engine defaults; Google-managed service agents
(``service-<num>@gcp-sa-*``) never appear there, only inside IAM policies.
The remaining patterns below are kept so the same predicate can also be applied
to members read out of a policy.
"""

from __future__ import annotations

DEFAULT_SA_PATTERNS = (
    "-compute@developer.gserviceaccount.com",
    "@appspot.gserviceaccount.com",
    "@cloudbuild.gserviceaccount.com",
    "@cloudservices.gserviceaccount.com",
    "@container-engine-robot.iam.gserviceaccount.com",
    "@gcp-sa-",
)


def is_default_service_account(email: str) -> bool:
    """Check if service account is a Google-created (non user-managed) account."""
    return any(pattern in email for pattern in DEFAULT_SA_PATTERNS)
