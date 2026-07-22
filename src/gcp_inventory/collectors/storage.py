"""Cloud Storage buckets."""

from __future__ import annotations

from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_storage_buckets",
    filename="cloud_storage_buckets.csv",
    header=("Project", "Bucket_Name"),
    scope=Scope.GLOBAL,
)
def cloud_storage_buckets(ctx: CollectorContext) -> RowIterator:
    from googleapiclient import discovery

    service = discovery.build(
        "storage", "v1", credentials=ctx.credentials, static_discovery=True
    )
    request = service.buckets().list(project=ctx.project)
    while request is not None:
        response = request.execute()
        for bucket in response.get("items", []):
            yield ctx.project, bucket["name"]
        request = service.buckets().list_next(previous_request=request, previous_response=response)
