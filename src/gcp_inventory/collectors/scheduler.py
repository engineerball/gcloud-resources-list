"""Cloud Scheduler jobs."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_scheduler",
    filename="cloud_scheduler.csv",
    header=("Project", "Scheduler"),
    scope=Scope.REGIONAL,
)
def cloud_scheduler(ctx: CollectorContext) -> RowIterator:
    from google.cloud import scheduler_v1beta1

    client = get_grpc_client(scheduler_v1beta1.CloudSchedulerClient, ctx.credentials)
    request = scheduler_v1beta1.ListJobsRequest(parent=ctx.parent)
    for job in client.list_jobs(request=request):
        yield ctx.project, job.name.split("/")[-1]
