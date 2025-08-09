import argparse
import json
import logging
import os
import subprocess
from contextlib import ExitStack

import typer

from slatedb_nbd_bench.bencher import Bencher
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


app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def goodbye(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


def cli():
    """Main CLI function to run the benchmarks."""
    args = parser.parse_args()

    results = []

    wal_enabled = [True, False] if args.test_wal_enabled else [None]

    for test in get_text_matrix(
        drivers=args.drivers,
        compression=args.compression,
        connections=args.connections,
        wal_enabled=wal_enabled,
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
            # Don't double count this one
            if test["label"] == "overall_test_duration":
                continue
            stats.push(test["elapsed"])
        result["summary"] = {
            "geometric_mean": stats.geometric_mean(),
            "geometric_standard_deviation": stats.geometric_standard_deviation(),
        }
        print(json.dumps(result, indent=2))

    def compare(key):
        values = getattr(args, key)

        if len(values) > 1:
            results_map = {value: RunningGeometricStats() for value in values}

            for test in result["tests"]:
                if test["label"] == "overall_test_duration":
                    continue
                results_map[result["config"][key]].push(test["elapsed"])

            print("=" * 40)
            print(f"Comparing {key}")
            for value in values:
                stats = results_map[value]
                print(f"Value: {value}")
                print(f"  Geometric Mean: {stats.geometric_mean()}")
                print(
                    f"  Geometric Standard Deviation: {stats.geometric_standard_deviation()}"
                )

    compare("driver")
    compare("compression")
    compare("connections")

    if len(wal_enabled) > 1:
        results_map = {enabled: RunningGeometricStats() for enabled in wal_enabled}
        for result in results:
            for test in result["tests"]:
                if test["label"] == "overall_test_duration":
                    continue
                results_map[result["config"]["wal_enabled"]].push(test["elapsed"])

        print("=" * 40)
        print("Comparing wal_enabled")
        for value in wal_enabled:
            stats = results_map[value]
            print(f"Value: {value}")
            print(f"  Geometric Mean: {stats.geometric_mean()}")
            print(
                f"  Geometric Standard Deviation: {stats.geometric_standard_deviation()}"
            )


if __name__ == "__main__":
    cli()
