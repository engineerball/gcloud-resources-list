"""Compute Engine instances."""

from __future__ import annotations

from gcp_inventory.client_cache import new_rest_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_compute_engine_instances",
    filename="cloud_compute_engine_instances.csv",
    header=("Project", "Instances", "Zone"),
    scope=Scope.GLOBAL,
)
def cloud_compute_engine_instances(ctx: CollectorContext) -> RowIterator:
    from google.cloud import compute_v1

    client = new_rest_client(compute_v1.InstancesClient, ctx.credentials)
    request = compute_v1.AggregatedListInstancesRequest(project=ctx.project)
    for zone_path, scoped in client.aggregated_list(request=request):
        for instance in scoped.instances:
            yield ctx.project, instance.name, zone_path.split("/")[-1]
