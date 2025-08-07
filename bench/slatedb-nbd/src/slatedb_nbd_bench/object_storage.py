from contextlib import contextmanager

import logging

import subprocess
from typing import Iterator


logger = logging.getLogger(__name__)


@contextmanager
def empty_bucket(
    bucket_name: str,
) -> Iterator[None]:
    """
    Empty a MinIO bucket using the MinIO CLI.

    Display space usage in the bucket when leaving context.
    """
    logger.info(f"Emptying MinIO bucket {bucket_name}...")
    subprocess.run(
        [
            "mcli",
            "rm",
            "--force",
            "--recursive",
            bucket_name,
        ],
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        check=True,
    )

    try:
        yield
    finally:
        # Show space usage in S3 bucket
        logger.info("Checking space usage in S3 bucket:")
        mcli = subprocess.run(
            ["mcli", "du", "truenas/zerofs"],
            stdout=subprocess.PIPE,
            check=True,
            encoding="utf-8",
        )
        print("Space usage:")
        print(mcli.stdout, end="")
