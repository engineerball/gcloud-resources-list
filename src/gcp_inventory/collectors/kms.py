"""Cloud KMS key rings and crypto keys."""

from __future__ import annotations

from gcp_inventory.client_cache import get_grpc_client
from gcp_inventory.registry import CollectorContext, RowIterator, Scope, collector


@collector(
    name="cloud_kms_keys",
    filename="cloud_kms_keys.csv",
    header=("Project", "KeyRing", "CryptoKey"),
    scope=Scope.REGIONAL,
)
def cloud_kms_keys(ctx: CollectorContext) -> RowIterator:
    from google.cloud import kms

    client = get_grpc_client(kms.KeyManagementServiceClient, ctx.credentials)
    key_rings = client.list_key_rings(request={"parent": ctx.parent})
    for key_ring in key_rings:
        crypto_keys = client.list_crypto_keys(request={"parent": key_ring.name})
        for crypto_key in crypto_keys:
            yield (
                ctx.project,
                key_ring.name.split("/")[-1],
                crypto_key.name.split("/")[-1],
            )
