import json
import logging
import os
import subprocess
from contextlib import ExitStack
from enum import Enum
from typing import Annotated

import typer

from slatedb_nbd_bench.bencher import Bencher, bench_print
from slatedb_nbd_bench.drivers.config import get_text_matrix
from slatedb_nbd_bench.drivers.slatedb_nbd import slate_db_background
from slatedb_nbd_bench.drivers.zerofs import setup_plan9, zerofs_background
from slatedb_nbd_bench.nbd import temporary_nbd_device
from slatedb_nbd_bench.object_storage import empty_bucket
from slatedb_nbd_bench.stats import RunningGeometricStats
from slatedb_nbd_bench.tests import bench_snapshot, bench_sync
from slatedb_nbd_bench.tests.files import (
    bench_linux_kernel_source_extraction,
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


app = typer.Typer(help="Command line interface for SlateDB NBD tests and benchmarking.")


@app.command()
def test():
    # Run end-to-end or integration tests
    pass


class Drivers(str, Enum):
    slatedb_nbd = "slatedb-nbd"
    zerofs = "zerofs"
    zerofs_plan9 = "zerofs-plan9"


class Compression(str, Enum):
    off = "off"
    zstd_fast = "zstd-fast"
    zstd = "zstd"

    # This is directly interpolated in CLI arguments
    def __str__(self):
        return self.value


@app.command()
def bench(
    *,
    drivers: Annotated[
        list[Drivers],
        typer.Option(
            help="Specify drivers to test.",
        ),
    ] = [Drivers.slatedb_nbd],  # noqa: B006
    compression: Annotated[
        list[Compression],
        typer.Option(
            help="Specify the compression algorithms to test.",
        ),
    ] = [Compression.zstd],  # noqa: B006
    connections: Annotated[
        list[int],
        typer.Option(
            help="Specify the number of connections to test.",
        ),
    ] = [1],  # noqa: B006
    test_wal_enabled: Annotated[
        bool, typer.Option(help="Enable WAL tests for SlateDB.")
    ] = False,
    test_object_store_cache: Annotated[
        bool, typer.Option(help="Enable object store caching tests for SlateDB.")
    ] = False,
):
    wal_enabled = [True, False] if test_wal_enabled else [None]
    object_store_cache = [True, False] if test_object_store_cache else [None]

    results = []

    for test in get_text_matrix(
        drivers=drivers,
        compression=compression,
        connections=connections,
        wal_enabled=wal_enabled,
        object_store_cache=object_store_cache,
    ):
        print("=" * 40)
        print("Starting new test run.")
        print(json.dumps(test, indent=2))
        bencher = Bencher()

        with ExitStack() as stack:
            stack.enter_context(push_pop_cwd(os.path.dirname(__file__)))
            stack.enter_context(empty_bucket("truenas/zerofs"))

            # This driver is quite different, so we handle it separately.
            if test["driver"] == "zerofs-plan9":
                # ZeroFS for plan 9
                # Need to pass:
                # * wal_enabled
                # * object_store_cache
                stack.enter_context(zerofs_background())

                # Plan 9
                stack.enter_context(setup_plan9())

                with bencher.bench("overall_test_duration"):
                    # Run the Linux kernel source extraction benchmark
                    bench_linux_kernel_source_extraction(bencher=bencher)

                    # This fails on Plan 9, so skip it for now.
                    # bench_sparse()

                    bench_write_big_zeroes(bencher=bencher)

                    with bencher.bench("sync"):
                        # Run the sync operation
                        subprocess.run(["sudo", "sync"], check=True)

                continue

            # Start the SlateDB NBD server in the background
            if test["driver"] == "slatedb-nbd":
                stack.enter_context(
                    slate_db_background(
                        wal_enabled=test.get("wal_enabled"),
                        object_store_cache=test.get("object_store_cache"),
                    )
                )
            elif test["driver"] == "zerofs":
                # Need to pass:
                # * wal_enabled
                # * object_store_cache
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

            with bench_print("overall_test_duration"):
                # Run the Linux kernel source extraction benchmark
                bench_linux_kernel_source_extraction(bencher=bencher)

                bench_sparse(bencher=bencher)

                bench_write_big_zeroes(bencher=bencher)

                bench_snapshot(zfs["dataset"], bencher=bencher)

                # bench_trim(zfs["pool"], bencher=bencher)

                # Some potential issues here?
                # bench_scrub(zfs["pool"], bencher=bencher)

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

        results.append(
            {
                "config": test,
                "tests": bencher.results,
            }
        )

    for result in results:
        stats = RunningGeometricStats()
        for test in result["tests"]:
            stats.push(test["elapsed"])
        result["summary"] = {
            "geometric_mean": stats.mean,
            "geometric_standard_deviation": stats.standard_deviation,
        }
        print(json.dumps(result, indent=2))

    def compare(key: str):
        values = set()
        for result in results:
            values.add(result["config"][key])

        values = sorted(values)

        if len(values) > 1:
            results_map = {value: RunningGeometricStats() for value in values}

            for test in result["tests"]:
                results_map[result["config"][key]].push(test["elapsed"])

            print("=" * 40)
            print(f"Comparing {key}")
            for value in values:
                stats = results_map[value]
                print(f"Value: {value}")
                print(f"  Geometric Mean: {stats.mean}")
                print(f"  Geometric Standard Deviation: {stats.standard_deviation}")

    test_parameters = [
        "driver",
        "compression",
        "connections",
        "wal_enabled",
        "object_store_cache",
    ]

    for test_condition in test_parameters:
        compare(test_condition)
