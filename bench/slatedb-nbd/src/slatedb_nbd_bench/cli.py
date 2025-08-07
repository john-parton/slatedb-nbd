import argparse
from contextlib import ExitStack, contextmanager

from dataclasses import dataclass, field
import itertools as it
import re
import json
import logging
import os
import secrets

import subprocess
import sys
import time
from typing import Iterator, NotRequired, TypedDict

from slatedb_nbd_bench.drivers.slatedb_nbd import slate_db_background
from slatedb_nbd_bench.drivers.zerofs import setup_plan9, zerofs_background
from slatedb_nbd_bench.nbd import temporary_nbd_device
from slatedb_nbd_bench.tests import bench_scrub, bench_snapshot, bench_sync, bench_trim
from slatedb_nbd_bench.tests.files import (
    bench_linux_kernel_source_extraction,
    bench_recursive_delete,
    bench_sparse,
    bench_write_big_zeroes,
)
from slatedb_nbd_bench.working_dir import push_pop_cwd
from slatedb_nbd_bench.zfs import temporary_zfs


logger = logging.getLogger(__name__)

# Configure logger to output to stdout
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[logging.StreamHandler()],
)


@contextmanager
def empty_bucket(
    bucket_name: str,
) -> Iterator[None]:
    """
    Empty an S3 bucket using the AWS CLI.
    """
    logger.info(f"Emptying S3 bucket {bucket_name}...")
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


class _TestConfig(TypedDict):
    driver: str
    compression: str | None
    encryption: bool
    ashift: NotRequired[int | None]
    block_size: NotRequired[
        int | None
    ]  # Really only a small number of values are appropriate
    slog_size: NotRequired[int | None]  # Only used for SlateDB NBD tests
    connection: NotRequired[int | None]  # Number of connections to use for NBD
    wal_enabled: NotRequired[bool | None]  # Whether to enable WAL for SlateDB NBD


DRIVER_DEFAULTS = {
    "zerofs": {
        "encryption": False,
    },
    "slatedb-nbd": {
        "encryption": True,
        "ashift": 12,
        "block_size": 4096,
    },
}

parser = argparse.ArgumentParser(
    description="Run benchmarks for SlateDB NBD and ZeroFS with various configurations."
)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add command line arguments to the parser.
    """
    # Should probably move 'bench plan 9' to drivers
    parser.add_argument(
        "--bench-plan-9",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--drivers",
        nargs="+",
        choices=["slatedb-nbd", "zerofs", "zerofs-plan9"],
        default=["slatedb-nbd"],
        help="Specify which drivers to run benchmarks for. Default is slatdb-nbd only.",
    )
    parser.add_argument(
        "--compression",
        nargs="+",
        choices=["off", "zstd-fast", "zstd"],
        default=["zstd"],
        help="Specify the compression algorithms to use. Default is zstd.",
    )
    parser.add_argument(
        "--connections",
        nargs="+",
        type=int,
        default=[1],
        help="Specify the number of connections to use for benchmarks. Default is 1.",
    )
    parser.add_argument(
        "--test-wal-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable WAL tests for SlateDB. Default is False.",
    )


add_arguments(parser)


def get_text_matrix(
    *,
    drivers: list[str],
    compression: list[str],
    connections: list[int],
    wal_enabled: bool | None = None,
) -> Iterator[_TestConfig]:
    conf = it.product(
        drivers,
        compression,
        connections,
        # Try both WAL on/off if testing, otherwise use defaults
        [True, False] if wal_enabled is None else [None],
    )

    for case_driver, case_compression, case_connections, case_wal_enabled in conf:
        yield {
            "driver": case_driver,
            "compression": None if case_compression == "off" else case_compression,
            "connections": case_connections,
            "wal_enabled": case_wal_enabled,
            **DRIVER_DEFAULTS.get(case_driver, {}),
        }


def cli():
    """Main CLI function to run the benchmarks."""

    args = parser.parse_args()

    for test in get_text_matrix(
        drivers=args.drivers,
        compression=args.compression,
        connections=args.connections,
    ):
        print("=" * 40)
        print("Starting new test run.")
        print(json.dumps(test, indent=2))
        bencher = Bencher(test)

        with ExitStack() as stack:
            stack.enter_context(push_pop_cwd(os.path.dirname(__file__)))
            stack.enter_context(empty_bucket("truenas/zerofs"))

            # This driver is quite different, so we handle it separately.
            if test["driver"] == "zerofs-plan9":
                # ZeroFS for plan 9
                stack.enter_context(zerofs_background())

                # Plan 9
                stack.enter_context(setup_plan9())

                with bencher.bench("overall_test_duration"):
                    # Run the Linux kernel source extraction benchmark
                    bench_linux_kernel_source_extraction(bencher=bencher)

                    bench_recursive_delete(bencher=bencher)

                    # This fails on Plan 9, so skip it for now.
                    # bench_sparse()

                    bench_write_big_zeroes(bencher=bencher)

                    with bencher.bench("sync"):
                        # Run the sync operation
                        subprocess.run(["sudo", "sync"], check=True)

                continue

            # Start the SlateDB NBD server in the background
            if test["driver"] == "slatedb-nbd":
                stack.enter_context(slate_db_background())
            elif test["driver"] == "zerofs":
                stack.enter_context(zerofs_background())
            else:
                raise ValueError(f"Unknown driver: {test['driver']}")

            # Create a temporary NBD device
            nbd_device = stack.enter_context(
                temporary_nbd_device(block_size=test.get("block_size"))
            )

            zfs = stack.enter_context(
                temporary_zfs(
                    device=nbd_device,
                    ashift=test.get("ashift"),
                    slog_size=test.get("slog_size"),
                    encryption=test.get("encryption"),
                    compression=test.get("compression"),
                )
            )

            with bencher.bench("overall_test_duration"):
                # Run the Linux kernel source extraction benchmark
                bench_linux_kernel_source_extraction(bencher=bencher)

                bench_recursive_delete(bencher=bencher)

                bench_sparse(bencher=bencher)

                bench_write_big_zeroes(bencher=bencher)

                bench_snapshot(zfs["dataset"], bencher=bencher)

                bench_trim(zfs["pool"], bencher=bencher)

                # Some potential issues here?
                bench_scrub(zfs["pool"], bencher=bencher)

                bench_sync(zfs["pool"], bencher=bencher)

            # Show how much data is used
            logger.info("Checking space usage in S3 bucket:")
            mcli = subprocess.run(
                ["mcli", "du", "truenas/zerofs"],
                stdout=subprocess.PIPE,
                check=True,
                encoding="utf-8",
            )
            print("Space usage:")
            print(mcli.stdout, end="")


if __name__ == "__main__":
    cli()
