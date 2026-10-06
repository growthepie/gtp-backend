"""Remove old analytics snapshots that contain private address/tag mappings."""

try:
    from oli_private_attesters import get_private_attester_hexes, require_private_attesters
except ModuleNotFoundError:
    from src.oli.api.oli_private_attesters import get_private_attester_hexes, require_private_attesters


def remove_private_analytics_files(s3, cloudfront, bucket, distribution_id):
    require_private_attesters()
    paths = [f"v1/oli/analytics/attester/{attester}.json" for attester in get_private_attester_hexes()]
    # Deleting the current object also places a delete marker in a versioned bucket.
    # This removes the current public object, rather than rewriting its labels.
    for path in paths:
        s3.delete_object(Bucket=bucket, Key=path)
    from uuid import uuid4
    result = cloudfront.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={"Paths": {"Quantity": len(paths), "Items": [f"/{path}" for path in paths]},
                           "CallerReference": str(uuid4())},
    )
    return result["Invalidation"]["Id"]
