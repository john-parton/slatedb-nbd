from contextlib import ExitStack, contextmanager

from math import log
import re
import json
import logging
import os
import secrets

import subprocess
import sys
import time
from typing import Iterator, NotRequired, TypedDict


logger = logging.getLogger(__name__)

# Configure logger to output to stdout
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[logging.StreamHandler()],
)


@contextmanager
def push_pop_cwd(new_cwd: str) -> Iterator[None]:
    original_cwd = os.getcwd()
    os.chdir(new_cwd)
    try:
        yield
    finally:
        os.chdir(original_cwd)


@contextmanager
def bench(label):
    start = time.perf_counter()

    try:
        yield
    finally:
        end = time.perf_counter()
        elapsed = end - start
        print(f"{label}: {elapsed:.3f} seconds")


@contextmanager
def temporary_nbd_device(
    *,
    port: int = 10809,
    block_size: int | None = None,
    device_index: int = 7,
    automatically_disconnect: bool = True,
) -> Iterator[str]:
    """
    Context manager to create a temporary NBD device.
    The device is created at the start and disconnected at the end.
    """
    device = f"/dev/nbd{device_index}"

    # Check if the NBD device is already in use
    nbd_client = subprocess.run(
        ["nbd-client", "-c", device],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        check=False,
    )

    clients = list(filter(None, nbd_client.stdout.splitlines()))

    if clients:
        if automatically_disconnect:
            logger.warning(
                f"NBD device {device} is already in use. Disconnecting existing client..."
            )
            # Disconnect the existing NBD client
            subprocess.run(["sudo", "nbd-client", "-d", device], check=True)
        else:
            logger.error(f"NBD device {device} is already in use.")
            raise RuntimeError(f"NBD device {device} is already in use.")

    options = []

    if block_size is not None:
        options.append(f"-b{block_size}")

    logger.debug(f"Connecting NBD device {device} on port {port}...")
    subprocess.run(
        [
            "sudo",
            "nbd-client",
            *options,
            "127.0.0.1",
            str(port),
            device,
            "-N",
            f"device_{port}",
        ],
        check=True,
    )
    logger.debug(f"NBD device {device} connected successfully.")

    try:
        yield device  # Yield control to the block of code using this context manager
    finally:
        logger.debug(f"Disconnecting NBD device {device}...")
        subprocess.run(["sudo", "nbd-client", "-d", device], check=True)
        logger.debug(f"NBD device {device} disconnected successfully.")


@contextmanager
def temporary_zpool(
    *, device: str, ashift: int | None = None, slog_size: int | None = None
):
    """
    Context manager to create a temporary ZFS pool.
    The pool is created at the start and destroyed at the end.
    """

    # Automatically generate a name
    name = f"testpool_{secrets.token_hex(4)}"

    options = [
        # This COULD clobber other pools that use NBD.
        # Should probably note that somewhere visible.
        "-f",
    ]

    if ashift is not None:
        options.extend(["-o", f"ashift={ashift}"])

    logger.debug(f"Creating ZFS pool {name} on device {device}...")
    subprocess.run(["sudo", "zpool", "create", *options, name, device], check=True)
    logger.debug(f"ZFS pool {name} created successfully.")

    if slog_size:
        # Create a file for the SLOG device
        slog_device = f"/tmp/{name}_slog"
        logger.debug(f"Creating SLOG device {slog_device} with size {slog_size}...")
        subprocess.run(["fallocate", "-l", str(slog_size), slog_device], check=True)
        logger.debug(f"Attaching SLOG device {slog_device} to pool {name}...")
        subprocess.run(["sudo", "zpool", "add", name, "log", slog_device], check=True)

    try:
        yield name  # Yield control to the block of code using this context manager
    finally:
        # Destroy the ZFS pool.
        logger.debug(f"Destroying ZFS pool {name}...")
        subprocess.run(["sudo", "zpool", "destroy", name], check=True)
        logger.debug(f"ZFS pool {name} destroyed successfully.")


@contextmanager
def temporary_zfs_dataset(
    *,
    pool: str,
    dataset: str = "test",
    encryption: bool = False,
    compression: str | None = None,
):
    """
    Context manager to create a temporary ZFS dataset.
    The dataset is created at the start and destroyed at the end.
    """

    name = f"{pool}/{dataset}"

    mountpoint = f"/mnt/{name}"

    options = [
        "-o",
        f"mountpoint={mountpoint}",
    ]

    # If encryption is enabled, make a temporary key
    if encryption:
        key_path = f"/tmp/zfs_{dataset}.key"

        key = secrets.token_hex()

        # Write to file
        with open(key_path, "wt") as key_file:
            key_file.write(key)

        options.extend(
            [
                "-o",
                "encryption=on",
                "-o",
                f"keylocation=file://{key_path}",
                "-o",
                "keyformat=passphrase",
            ]
        )

    if compression:
        options.extend(["-o", f"compression={compression}"])

    logger.debug(f"Creating ZFS dataset {name}...")
    subprocess.run(["sudo", "zfs", "create", *options, name], check=True)
    logger.debug(f"ZFS dataset {name} created successfully.")

    # chown the mountpoint to the current user
    current_user = subprocess.run(
        ["whoami"], stdout=subprocess.PIPE, encoding="utf-8", check=True
    ).stdout.strip()

    current_group = subprocess.run(
        ["id", "-gn"], stdout=subprocess.PIPE, encoding="utf-8", check=True
    ).stdout.strip()

    logger.debug(
        f"Changing ownership of {mountpoint} to {current_user}:{current_group}..."
    )
    subprocess.run(
        ["sudo", "chown", f"{current_user}:{current_group}", mountpoint], check=True
    )

    os.chdir(mountpoint)

    try:
        yield name
    finally:
        # Change the working directory to the root
        os.chdir("/")
        # Wait a few seconds to ensure all operations are complete
        logger.debug("Waiting for ZFS operations to complete...")
        time.sleep(2)
        # Spit out anything that's still using the mountpoint
        subprocess.run(["lsof", "+D", mountpoint], check=False)


@contextmanager
def zerofs_background(*, automatically_kill: bool = True) -> Iterator[subprocess.Popen]:
    """
    Context manager to run ZeroFS in the background.
    The process is started at the start and killed at the end.
    """

    # Check if a process is already running
    existing_process = subprocess.run(
        ["pgrep", "-f", "^target/release/zerofs$"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        check=False,
    )

    if existing_process.stdout.splitlines():
        if automatically_kill:
            logger.warning("ZeroFS is already running. Killing existing process...")
            # Kill the existing process
            subprocess.run(["pkill", "-f", "^target/release/zerofs$"], check=True)
        else:
            logger.error("ZeroFS is already running.")
            raise RuntimeError(
                "ZeroFS is already running. Please stop it before starting a new instance."
            )

    cwd = os.getcwd()

    os.chdir("/tmp")

    # Check if 'ZeroFS' directory exists
    if not os.path.exists("ZeroFS"):
        logger.debug("Cloning ZeroFS repository...")
        subprocess.run(["git", "clone", "git@github.com:Barre/ZeroFS.git", "ZeroFS"])
        os.chdir("ZeroFS")
    else:
        os.chdir("ZeroFS")
        logger.debug("Pulling latest changes for ZeroFS repository...")
        subprocess.run(["git", "pull"], check=True)

    # Build ZeroFS in release mode
    logger.debug("Building ZeroFS in release mode...")
    subprocess.run(
        ["cargo", "build", "--profile", "release"],
        check=True,
    )

    zerofs_env = os.environ.copy()
    zerofs_env["AWS_ALLOW_HTTP"] = "true"
    zerofs_env["SLATEDB_CACHE_DIR"] = "/tmp/zerofs-cache"
    zerofs_env["SLATEDB_CACHE_SIZE_GB"] = "2"
    zerofs_env["ZEROFS_ENCRYPTION_PASSWORD"] = "secret"
    zerofs_env["ZEROFS_NBD_PORTS"] = "10809"
    zerofs_env["ZEROFS_NBD_DEVICE_SIZES_GB"] = "3"

    # Start ZeroFS in the background, should be relatively quick because
    # we built it above
    logger.debug("Starting ZeroFS in the background...")
    process = subprocess.Popen(
        [
            "cargo",
            "run",
            "--profile",
            "release",
            "--",
            "s3://zerofs",
        ],
        env=zerofs_env,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )

    # Restore previous working directory
    os.chdir(cwd)

    try:
        # Wait a bit for it to start
        logger.debug("Waiting for ZeroFS to start...")
        time.sleep(5)
        logger.debug("ZeroFS started successfully.")
        yield process  # Yield control to the block of code using this context manager
    finally:
        logger.debug("Stopping ZeroFS...")
        # Kill the ZeroFS process
        process.terminate()
        logger.debug("Waiting for ZeroFS to stop...")
        process.wait()
        logger.debug("ZeroFS stopped.")


@contextmanager
def slate_db_background(
    *, automatically_kill: bool = True
) -> Iterator[subprocess.Popen]:
    """
    Context manager to run SlateDB in the background.
    The process is started at the start and killed at the end.
    """

    # Check if a process is already running
    existing_process = subprocess.run(
        ["pgrep", "-f", "^target/release/slatedb_nbd$"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        check=False,
    )

    if existing_process.stdout.splitlines():
        if automatically_kill:
            logger.warning(
                "SlateDB NBD server is already running. Killing existing process..."
            )
            # Kill the existing process
            subprocess.run(["pkill", "-f", "^target/release/slatedb_nbd$"], check=True)
        else:
            logger.error("SlateDB NBD server is already running.")
            raise RuntimeError(
                "SlateDB NBD server is already running. Please stop it before starting a new instance."
            )

    # Start SlateDB in the background
    logger.debug("Starting SlateDB NBD server in the background...")
    process = subprocess.Popen(
        ["cargo", "run", "--profile", "release"],
    )

    try:
        # Wait a bit for it to start
        logger.debug("Waiting for SlateDB NBD server to start...")
        time.sleep(5)
        logger.debug("SlateDB NBD server started successfully.")
        yield process  # Yield control to the block of code using this context manager
    finally:
        logger.debug("Stopping SlateDB NBD server...")
        # Kill the SlateDB process
        process.terminate()
        logger.debug("Waiting for SlateDB NBD server to stop...")
        process.wait()
        logger.debug("SlateDB NBD server stopped.")


def empty_bucket(
    bucket_name: str,
) -> None:
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


def bench_linux_kernel_source_extraction() -> None:
    """
    Benchmarks the extraction of the Linux kernel source code.
    This is a placeholder for the actual benchmarking logic.
    """
    # Use wget to put the Linux kernel source in the current directory
    kernel_version = "6.15.6"
    kernel_url = (
        f"https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-{kernel_version}.tar.xz"
    )
    logger.info(f"Downloading Linux kernel source from {kernel_url}...")
    subprocess.run(["wget", "-q", kernel_url], check=True)
    logger.info(f"Linux kernel source downloaded: linux-{kernel_version}.tar.xz")

    # Extract the downloaded tarball
    logger.info("Extracting Linux kernel source...")
    with bench("linux_kernel_source_extraction"):
        # Use tar to extract the kernel source
        # Using pixz for parallel decompression if available
        subprocess.run(
            # "time" doesn't work here -- "No such file or directory: time"
            ["tar", "-I", "pixz", "-xf", f"linux-{kernel_version}.tar.xz"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    logger.info("Linux kernel source extracted.")


def bench_recursive_delete() -> None:
    """
    Benchmarks the recursive deletion of the Linux kernel source directory.
    This is a placeholder for the actual benchmarking logic.
    """
    kernel_version = "6.15.6"
    kernel_dir = f"linux-{kernel_version}"

    logger.info(f"Deleting extracted Linux kernel source directory {kernel_dir}...")
    with bench("recursive_delete"):
        subprocess.run(["rm", "-rf", kernel_dir], check=True)
    logger.info(f"Linux kernel source directory {kernel_dir} deleted.")


def bench_sync(zpool: str) -> None:
    """
    Benchmarks the sync operation.
    This is a placeholder for the actual benchmarking logic.
    """
    logger.info("Running sync operation...")
    with bench("sync"):
        subprocess.run(["sudo", "sync"], check=True)
    with bench("zpool sync"):
        subprocess.run(["sudo", "zpool", "sync", zpool], check=True)
    logger.info("Sync operation completed.")


def bench_trim(zpool: str) -> None:
    """
    Benchmarks the TRIM operation.
    This is a placeholder for the actual benchmarking logic.
    """
    logger.info("Running TRIM operation...")
    subprocess.run(["sudo", "zpool", "trim", zpool], check=True)

    # Poll the status of the TRIM operation
    # We're going to get very poor resolution here because
    # we have to poll, and I don't know how to get the
    # actual duration of a TRIM operation as reported by ZFS.
    # (Compare scrubbing, which has a "time" field in the status output.)
    with bench("wait_for_trim_completion"):
        for attempt in range(1, 61):
            logger.debug(f"Checking TRIM status (attempt #{attempt})...")
            status = subprocess.run(
                ["zpool", "status", zpool],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                check=True,
            )
            if "trimming" not in status.stdout.lower():
                break
            logger.debug("TRIM operation in progress...")
            time.sleep(1)

    logger.info("TRIM operation completed.")


def bench_scrub(zpool: str) -> None:
    """
    Benchmarks the scrub operation.
    This is a placeholder for the actual benchmarking logic.
    """
    logger.info("Starting ZFS scrub...")
    subprocess.run(["sudo", "zpool", "scrub", zpool], check=True)

    # Poll the status of the scrub operation
    with bench("wait_for_scrub_completion"):
        for attempt in range(1, 601):
            # logger.debug(f"Checking scrub status (attempt #{attempt})...")
            status = subprocess.run(
                ["zpool", "status", zpool],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                check=True,
            )
            if "scrub in progress" not in status.stdout.lower():
                break
            # logger.debug("Scrub operation in progress...")
            time.sleep(1)

    # Get final scrub status
    final_status = subprocess.run(
        ["zpool", "status", zpool],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        check=True,
    )

    print(final_status.stdout, file=sys.stderr)

    # Regex out 'scrub repaired 0B in 00:00:39 with 0 errors'

    scrub_repaired_regex = r"scrub repaired (?P<repaired>[^ ]+) in (?P<duration>\d{2}:\d{2}:\d{2}) with (?P<errors>\d+) errors"

    match = re.search(scrub_repaired_regex, final_status.stdout)

    if not match:
        print(final_status.stdout)
        raise RuntimeError("Failed to parse scrub status.")

    print(f"scrub_status: {match.group(0)}.")

    logger.info("Scrub operation completed.")


def bench_sparse():
    with bench("sparse_file_creation"):
        subprocess.run(["fallocate", "-l", "1G", "sparse.bin"], check=True)
    # Consider testing reflink here, but it requires a ZFS dataset with bclone
    # enabled and the client-tools installed.


def bench_write_big_zeroes():
    with bench("write_big_zeroes"):
        subprocess.run(
            ["dd", "if=/dev/zero", "of=big_zeroes.bin", "bs=1M", "count=1024"],
            check=True,
        )


def bench_snapshot(dataset: str) -> None:
    logger.info("Creating ZFS snapshot...")
    with bench("zfs_snapshot"):
        subprocess.run(
            ["sudo", "zfs", "snapshot", f"{dataset}@after-kernel"], check=True
        )
    logger.info("ZFS snapshot created successfully.")


class _TestConfig(TypedDict):
    driver: str
    compression: str | None
    encryption: bool
    ashift: NotRequired[int | None]
    block_size: NotRequired[
        int | None
    ]  # Really only a small number of values are appropriate
    slog_size: NotRequired[int | None]  # Only used for SlateDB NBD tests


TESTS: list[_TestConfig] = [
    {
        "driver": "zerofs",
        "compression": None,
        "encryption": False,
    },
    {
        "driver": "zerofs",
        "compression": "zstd",
        "encryption": False,
    },
    {
        "driver": "slatedb-nbd",
        "compression": "zstd-fast",
        "encryption": True,
        "ashift": 12,
        "block_size": 4096,
    },
    {
        "driver": "slatedb-nbd",
        # zstd is equivalent to zstd-3
        "compression": "zstd",
        "encryption": True,
        "ashift": 12,
        "block_size": 4096,
    },
    # zstd-9 didn't perform great, but it's here
    # if you want to run it.
    # {
    #     "driver": "slatedb-nbd",
    #     "compression": "zstd-9",
    #     "encryption": True,
    #     "ashift": 12,
    #     "block_size": 4096,
    # },
]


def main():
    for test in TESTS:
        print("=" * 40)
        print("Starting new test run.")
        print(json.dumps(test, indent=2))

        empty_bucket("truenas/zerofs")

        with ExitStack() as stack:
            stack.enter_context(push_pop_cwd(os.path.dirname(__file__)))

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

            zpool = stack.enter_context(
                temporary_zpool(
                    device=nbd_device,
                    ashift=test.get("ashift"),
                    slog_size=test.get("slog_size"),
                )
            )

            dataset = stack.enter_context(
                temporary_zfs_dataset(
                    pool=zpool,
                    encryption=test.get("encryption"),
                    compression=test.get("compression"),
                )
            )

            with bench("overall_test_duration"):
                # Run the Linux kernel source extraction benchmark
                bench_linux_kernel_source_extraction()

                bench_recursive_delete()

                bench_sparse()

                bench_write_big_zeroes()

                bench_snapshot(dataset)

                bench_trim(zpool)

                # Some potential issues here?
                # bench_scrub(zpool)

                bench_sync(zpool)

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
    main()
