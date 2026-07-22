"""Cloud Run services."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_run_services",
    filename="cloud_run_services.csv",
    header=("Project", "Service"),
    scope=Scope.REGIONAL,
)
def cloud_run_services(ctx: CollectorContext) -> RowIterator:
    from google.cloud import run_v2

    client = get_grpc_client(run_v2.ServicesClient, ctx.credentials)
    request = run_v2.ListServicesRequest(parent=ctx.parent)
    for service in client.list_services(request=request):
        yield ctx.project, service.name.split("/")[-1]
