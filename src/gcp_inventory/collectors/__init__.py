"""Collector modules. Importing this package registers every collector.

Each module keeps its google-cloud import inside the collector function:
a missing or broken client library breaks only that collector's tasks,
never the whole run (deliberate, see CLAUDE.md).
"""

from gcp_inventory.collectors import (
    bigquery,  # noqa: F401
    cloud_functions,  # noqa: F401
    cloud_run,  # noqa: F401
    compute,  # noqa: F401
    dataform,  # noqa: F401
    iam,  # noqa: F401
    kms,  # noqa: F401
    scheduler,  # noqa: F401
    storage,  # noqa: F401
    workflows,  # noqa: F401
)
