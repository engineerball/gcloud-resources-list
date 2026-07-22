"""Dataform repositories and workspaces."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_dataform",
    filename="cloud_dataform.csv",
    header=("Project", "Repository", "Workspace"),
    scope=Scope.REGIONAL,
)
def cloud_dataform(ctx: CollectorContext) -> RowIterator:
    from google.cloud import dataform_v1beta1

    client = get_grpc_client(dataform_v1beta1.DataformClient, ctx.credentials)
    request = dataform_v1beta1.ListRepositoriesRequest(parent=ctx.parent)
    for repository in client.list_repositories(request=request):
        for workspace in client.list_workspaces(parent=repository.name):
            yield (
                ctx.project,
                repository.name.split("/")[-1],
                workspace.name.split("/")[-1],
            )
