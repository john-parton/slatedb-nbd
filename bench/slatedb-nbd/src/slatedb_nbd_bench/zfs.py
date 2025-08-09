import logging
import os
import secrets
import subprocess
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import TypedDict

logger = logging.getLogger(__name__)


# Just basic information
class Info(TypedDict):
    pool: str
    dataset: str
    mountpoint: str


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
        with open(key_path, "w") as key_file:
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
        yield {
            "name": name,
            "mountpoint": mountpoint,
        }
    finally:
        # Change the working directory to the root
        os.chdir("/")
        # Wait a few seconds to ensure all operations are complete
        logger.debug("Waiting for ZFS operations to complete...")
        time.sleep(2)
        # Spit out anything that's still using the mountpoint
        subprocess.run(["lsof", "+D", mountpoint], check=False)


@contextmanager
def temporary_zfs(
    *,
    device: str,
    ashift: int | None = None,
    slog_size: int | None = None,
    encryption: bool = False,
    compression: str | None = None,
    dataset: str = "test",
) -> Iterator[Info]:
    """
    Context manager to create a temporary ZFS pool and dataset.
    The pool and dataset are created at the start and destroyed at the end.
    """
    with ExitStack() as stack:
        pool = stack.enter_context(
            temporary_zpool(device=device, ashift=ashift, slog_size=slog_size)
        )
        info = stack.enter_context(
            temporary_zfs_dataset(
                pool=pool,
                dataset=dataset,
                encryption=encryption,
                compression=compression,
            )
        )

        yield {
            "pool": pool,
            "dataset": info["name"],
            "mountpoint": info["mountpoint"],
        }
