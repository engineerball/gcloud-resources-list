"""BigQuery data policies, datasets, and tables."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client, new_rest_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_bigquery_datapolicies",
    filename="cloud_bigquery_datapolicies.csv",
    header=("Project", "Policies"),
    scope=Scope.REGIONAL,
)
def cloud_bigquery_datapolicies(ctx: CollectorContext) -> RowIterator:
    from google.cloud import bigquery_datapolicies_v1

    client = get_grpc_client(
        bigquery_datapolicies_v1.DataPolicyServiceClient, ctx.credentials
    )
    request = bigquery_datapolicies_v1.ListDataPoliciesRequest(parent=ctx.parent)
    for policy in client.list_data_policies(request=request):
        yield ctx.project, policy.name


@collector(
    name="cloud_bigquery_datasets",
    filename="cloud_bigquery_datasets.csv",
    header=("Project", "Dataset"),
    scope=Scope.GLOBAL,
)
def cloud_bigquery_datasets(ctx: CollectorContext) -> RowIterator:
    from google.cloud import bigquery

    client = new_rest_client(
        bigquery.Client, ctx.credentials, project=ctx.project
    )
    for dataset in client.list_datasets():
        yield ctx.project, dataset.dataset_id


@collector(
    name="cloud_bigquery_datasets_tables",
    filename="cloud_bigquery_datasets_tables.csv",
    header=("Project", "Dataset", "Table"),
    scope=Scope.GLOBAL,
)
def cloud_bigquery_datasets_tables(ctx: CollectorContext) -> RowIterator:
    from google.cloud import bigquery

    client = new_rest_client(
        bigquery.Client, ctx.credentials, project=ctx.project
    )
    for dataset in client.list_datasets():
        for table in client.list_tables(dataset.dataset_id):
            yield table.project, table.dataset_id, table.table_id
