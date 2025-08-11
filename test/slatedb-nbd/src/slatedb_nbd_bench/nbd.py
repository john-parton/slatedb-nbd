import logging
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def temporary_nbd_device(
    *,
    port: int = 10809,
    block_size: int | None = None,
    device_index: int = 5,
    automatically_disconnect: bool = True,
    connections: int | None = None,
    device_name: str | None = None,
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

    if connections is not None:
        options.append(f"-c{connections}")

    if device_name is not None:
        options.extend(["-n", device_name])

    logger.debug(f"Connecting NBD device {device} on port {port}...")
    subprocess.run(
        [
            "sudo",
            "nbd-client",
            *options,
            "127.0.0.1",
            str(port),
            device,
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
