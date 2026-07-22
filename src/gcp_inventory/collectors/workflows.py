"""Cloud Workflows."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_workflows",
    filename="cloud_workflows.csv",
    header=("Project", "Service"),
    scope=Scope.REGIONAL,
)
def cloud_workflows(ctx: CollectorContext) -> RowIterator:
    from google.cloud import workflows_v1

    client = get_grpc_client(workflows_v1.WorkflowsClient, ctx.credentials)
    request = workflows_v1.ListWorkflowsRequest(parent=ctx.parent)
    for workflow in client.list_workflows(request=request):
        yield ctx.project, workflow.name.split("/")[-1]
