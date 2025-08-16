from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
from contextlib import AsyncExitStack, ExitStack, contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Annotated, Self

import click
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

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

logger = logging.getLogger(__name__)

# Configure logger to output to stdout
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[logging.StreamHandler()],
)


app = typer.Typer(help="Command line interface for SlateDB NBD tests and benchmarking.")


@contextmanager
def zfs_on_nbd_driver(*, base_driver: Driver, **kwargs):
    # Start the SlateDB NBD server in the background
    with ExitStack() as stack:
        if base_driver == Driver.slatedb_nbd:
            stack.enter_context(
                slate_db_background(
                    wal_enabled=kwargs.get("wal_enabled"),
                    object_store_cache=kwargs.get("object_store_cache"),
                )
            )
        elif base_driver == Driver.zerofs:
            # Need to pass:
            # * wal_enabled
            # * object_store_cache
            stack.enter_context(zerofs_background())
        else:
            msg = f"Unknown driver: {base_driver}"
            raise ValueError(msg)

        nbd_device_name = "device_10809" if base_driver == "zerofs" else None

        # Create a temporary NBD device
        nbd_device = stack.enter_context(
            temporary_nbd_device(
                block_size=kwargs.get("block_size"),
                device_name=nbd_device_name,
            )
        )

        zfs = stack.enter_context(
            temporary_zfs(
                device=nbd_device,
                ashift=kwargs.get("ashift"),
                slog_size=kwargs.get("slog_size"),
                encryption=kwargs.get("encryption"),
                compression=kwargs.get("compression"),
                zfs_sync=kwargs.get("zfs_sync"),
            )
        )

        try:
            yield zfs
        finally:
            pass


@app.command()
def test():
    # Run end-to-end or integration tests
    pass


class Driver(str, Enum):
    slatedb_nbd = "slatedb-nbd"
    zerofs = "zerofs"
    zerofs_plan9 = "zerofs-plan9"
    folder = "folder"


class Compression(str, Enum):
    off = "off"
    zstd_fast = "zstd-fast"
    zstd = "zstd"

    # This is directly interpolated in CLI arguments
    def __str__(self):
        return self.value


class ZFSSync(str, Enum):
    disabled = "disabled"
    standard = "standard"
    always = "always"

    def __str__(self):
        return self.value


def coro[**P, R](f: Callable[P, Coroutine[None, None, R]]) -> Callable[P, R]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@dataclass
class Config:
    bucket_name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str

    @classmethod
    def from_env(cls) -> Self:
        d = {
            key: os.environ.get(key)
            for key in (
                "AWS_ENDPOINT",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_BUCKET_NAME",
            )
        }

        missing = [key for key, value in d.items() if not value]

        if any(missing):
            msg = f"Missing required environment variables: {', '.join(missing)}"
            raise ValueError(msg)

        return cls(
            bucket_name=d["AWS_BUCKET_NAME"],  # pyright: ignore[reportArgumentType]
            endpoint_url=d["AWS_ENDPOINT"],  # pyright: ignore[reportArgumentType]
            access_key_id=d["AWS_ACCESS_KEY_ID"],  # pyright: ignore[reportArgumentType]
            secret_access_key=d["AWS_SECRET_ACCESS_KEY"],  # pyright: ignore[reportArgumentType]
        )


@app.command()
@coro
async def bench(
    *,
    drivers: Annotated[
        list[Driver],
        typer.Option(
            help="Specify drivers to test.",
        ),
    ] = [Driver.slatedb_nbd],  # noqa: B006
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
    test_zfs_sync: Annotated[
        bool, typer.Option(help="Enable ZFS sync tests for SlateDB.")
    ] = False,
    zfs_slog: Annotated[
        int | None, typer.Option(help="Set ZFS slog to specified size in gigabytes.")
    ] = None,
    test_folder: Annotated[
        str | None,
        typer.Option(
            help=(
                "Location of folder (local filesystem or other externally mounted fs) to test. "
                "Required for folder driver."
            )
        ),
    ] = None,
):
    wal_enabled = [True, False] if test_wal_enabled else [None]
    object_store_cache = [True, False] if test_object_store_cache else [None]
    zfs_sync = (
        [ZFSSync.disabled, ZFSSync.standard, ZFSSync.always]
        if test_zfs_sync
        else [None]
    )
    slog_size = [zfs_slog]

    results = []

    # In order to give an apples-to-apples comparison, the ZFS specific tests are
    # disabled if one of the drivers doesn't support it
    disable_zfs_tests: bool = Driver.folder in drivers or Driver.zerofs_plan9 in drivers

    # If the folder driver is being used, make sure that the test_folder option is passed
    # and explictly ask the user to confirm, because it is absolutely going to write
    # and delete data to that folder
    if Driver.folder in drivers:
        if not test_folder:
            msg = "test_folder option must be specified when using folder driver"
            raise ValueError(msg)

        click.confirm(
            (
                f"You are about to run tests that will write to the folder at {test_folder}. "
                "Existing data in the folder may be lost. Are you sure you want to continue?"
            ),
            abort=True,
        )

    for key in (
        "AWS_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_BUCKET_NAME",
    ):
        if key not in os.environ:
            msg = f"Missing required environment variable: {key}"
            raise ValueError(msg)

    bucket_name = os.environ["AWS_BUCKET_NAME"]
    endpoint_url = os.environ["AWS_ENDPOINT"]
    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    bucket_name = os.environ["AWS_BUCKET_NAME"]

    # Confirm that the bucket is going to get nuked
    if any(
        s3_driver in drivers
        for s3_driver in (Driver.slatedb_nbd, Driver.zerofs, Driver.zerofs_plan9)
    ):
        click.confirm(
            (
                f"You are about to run tests that will delete the bucket {bucket_name}. "
                "Existing data in the bucket will be lost. Are you sure you want to continue?"
            ),
            abort=True,
        )

    # Ask for sudo pass now. It will be needed soon
    subprocess.run(["sudo", "echo", "Thanks"], check=True, stdout=subprocess.DEVNULL)

    for test in get_text_matrix(
        drivers=drivers,
        compression=compression,
        connections=connections,
        wal_enabled=wal_enabled,
        object_store_cache=object_store_cache,
        zfs_sync=zfs_sync,
        slog_size=slog_size,
    ):
        print("=" * 40)
        print("Starting new test run.")
        print(json.dumps(test, indent=2))
        bencher = Bencher()

        async with AsyncExitStack() as stack:
            stack.enter_context(push_pop_cwd(os.path.dirname(__file__)))
            await stack.enter_async_context(
                empty_bucket(
                    bucket_name,
                    endpoint_url=endpoint_url,
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                )
            )
            continue

            # This driver is quite different, so we handle it separately.
            if test["driver"] == "zerofs-plan9":
                # ZeroFS for plan 9
                # Need to pass:
                # * wal_enabled
                # * object_store_cache
                stack.enter_context(zerofs_background())

                # Plan 9
                stack.enter_context(setup_plan9())

                with bench_print("overall_test_duration"):
                    # Run the Linux kernel source extraction benchmark
                    bench_linux_kernel_source_extraction(bencher=bencher)

                    # This fails on Plan 9, so skip it for now.
                    # bench_sparse()

                    bench_write_big_zeroes(bencher=bencher)

                    with bencher.bench("sync"):
                        # Run the sync operation
                        subprocess.run(["sudo", "sync"], check=True)

                continue

            if test["driver"] == "folder":
                stack.enter_context(push_pop_cwd(test_folder))

                with bench_print("overall_test_duration"):
                    # Run the Linux kernel source extraction benchmark
                    bench_linux_kernel_source_extraction(bencher=bencher)

                    bench_sparse(bencher=bencher)

                    bench_write_big_zeroes(bencher=bencher)
                continue

            zfs = stack.enter_context(
                zfs_on_nbd_driver(
                    base_driver=test["driver"],
                    **test,
                )
            )

            nbd_device_name = "device_10809" if test["driver"] == "zerofs" else None

            # Create a temporary NBD device
            nbd_device = stack.enter_context(
                temporary_nbd_device(
                    block_size=test.get("block_size"),
                    device_name=nbd_device_name,
                )
            )

            zfs = stack.enter_context(
                temporary_zfs(
                    device=nbd_device,
                    ashift=test.get("ashift"),
                    slog_size=test.get("slog_size"),
                    encryption=test.get("encryption"),
                    compression=test.get("compression"),
                    zfs_sync=test.get("zfs_sync"),
                )
            )

            with bench_print("overall_test_duration"):
                # Run the Linux kernel source extraction benchmark
                bench_linux_kernel_source_extraction(bencher=bencher)

                bench_sparse(bencher=bencher)

                bench_write_big_zeroes(bencher=bencher)

                if not disable_zfs_tests:
                    bench_snapshot(zfs["dataset"], bencher=bencher)

                    # bench_trim(zfs["pool"], bencher=bencher)

                    # Some potential issues here?
                    # bench_scrub(zfs["pool"], bencher=bencher)

                    bench_sync(zfs["pool"], bencher=bencher)

            # Show how much data is used
            logger.info("Checking space usage in S3 bucket:")
            mcli = subprocess.run(
                ["mcli", "du", f"{mcli_alias}/{mcli_bucket}"],
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

            for result in results:
                stats = results_map[result["config"][key]]
                for test in result["tests"]:
                    stats.push(test["elapsed"])

            print("=" * 40)
            print(f"Comparing {key}")
            for key, stats in results_map.items():
                print(f"Value: {key}")
                print(f"  Geometric Mean: {stats.mean}")
                print(f"  Geometric Standard Deviation: {stats.standard_deviation}")

    test_parameters = [
        "driver",
        "compression",
        "connections",
        "wal_enabled",
        "object_store_cache",
        "zfs_sync",
    ]

    for test_condition in test_parameters:
        compare(test_condition)
