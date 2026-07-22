"""Cloud Functions."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_functions",
    filename="cloud_functions.csv",
    header=("Project", "Function"),
    scope=Scope.REGIONAL,
)
def cloud_functions(ctx: CollectorContext) -> RowIterator:
    from google.cloud import functions_v2

    client = get_grpc_client(functions_v2.FunctionServiceClient, ctx.credentials)
    request = functions_v2.ListFunctionsRequest(parent=ctx.parent)
    for function in client.list_functions(request=request):
        yield ctx.project, function.name.split("/")[-1]
