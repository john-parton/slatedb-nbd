from contextlib import contextmanager

import logging
import os

import subprocess
import time
from typing import Iterator


logger = logging.getLogger(__name__)


@contextmanager
def slate_db_background(
    *,
    automatically_kill: bool = True,
    wal_enabled: bool | None = None,
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

    slate_db_env = os.environ.copy()

    if wal_enabled is not None:
        slate_db_env["SLATEDB_WAL_ENABLED"] = "true" if wal_enabled else "false"

    # Build SlateDB in release mode
    logger.debug("Building SlateDB in release mode...")
    subprocess.run(
        ["cargo", "build", "--profile", "release"],
        check=True,
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
