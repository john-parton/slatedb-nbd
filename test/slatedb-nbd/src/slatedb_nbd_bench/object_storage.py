import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aioboto3

logger = logging.getLogger(__name__)


# # Item is like this
# {
#     "Key": "tmp/test_db/manifest/00000000000000000004.manifest",
#     "LastModified": datetime.datetime(
#         2025, 8, 12, 3, 28, 37, 615000, tzinfo=tzutc()
#     ),
#     "ETag": '"da25836a7ff83a8aa970c93499322da9"',
#     "Size": 106,
#     "StorageClass": "STANDARD",
# }


@asynccontextmanager
async def empty_bucket(
    bucket_name: str, *, endpoint_url: str, secret_access_key: str, access_key_id: str
) -> AsyncIterator[None]:
    """
    Empty an S3 compatible bucket using aioboto3.

    Display space usage in the bucket when leaving context.
    """
    session = aioboto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )

    async with session.client(
        "s3",
        endpoint_url=endpoint_url,
    ) as s3:
        paginator = s3.get_paginator("list_objects_v2")

        # We could have some additional parallelism here
        async for page in paginator.paginate(
            Bucket=bucket_name,
            Prefix="",
        ):
            # Empty page / empty bucket
            if "Contents" not in page:
                continue
            objects_to_delete = [{"Key": item["Key"]} for item in page["Contents"]]
            await s3.delete_objects(
                Bucket=bucket_name, Delete={"Objects": objects_to_delete}
            )

        try:
            yield
        finally:
            total_size: int = 0

            paginator = s3.get_paginator("list_objects_v2")

            # We could have some additional parallelism here
            async for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix="",
            ):
                if "Contents" in page:
                    total_size += sum(item["Size"] for item in page["Contents"])

            print(
                f"Total size of objects in bucket '{bucket_name}': {total_size} bytes"
            )
