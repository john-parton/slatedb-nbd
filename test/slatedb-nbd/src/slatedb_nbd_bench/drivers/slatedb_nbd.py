import logging
import os
import secrets
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def slate_db_background(
    *,
    automatically_kill: bool = True,
    wal_enabled: bool | None = None,
    object_store_cache: bool | None = None,
    use_working_dir: bool = True,
) -> Iterator[None]:
    """
    Context manager to run SlateDB in the background.
    The process is started at the start and killed at the end.
    """
    # Create temporary directory, checkout main
    if not use_working_dir:
        # Change dir to tmp
        os.chdir("/tmp")
        # Clone
        if os.path.exists("slatedb-nbd"):
            os.chdir("slatedb-nbd")
            logger.debug("Pulling latest changes for SlateDB-NBD repository...")
            subprocess.run(["git", "pull"], check=True)
        else:
            logger.debug("Cloning SlateDB-NBD repository...")
            subprocess.run(
                ["git", "clone", "git@github.com:john-parton/slatedb-nbd.git"],
                check=True,
            )
            os.chdir("slatedb-nbd")

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

    # The presence of 'root_folder' determines whether the cache is on or off
    if object_store_cache is True:
        suffix = secrets.token_hex(4)
        slate_db_env["SLATEDB_OBJECT_STORE_CACHE_OPTIONS"] = (
            f'{{root_folder=/tmp/slatedb-object-store-cache_{suffix},max_cache_size_bytes=17179869184,part_size_bytes=4194304,scan_interval="1h"}}'
        )
    elif object_store_cache is False:
        slate_db_env["SLATEDB_OBJECT_STORE_CACHE_OPTIONS"] = (
            '{root_folder=None,max_cache_size_bytes=17179869184,part_size_bytes=4194304,scan_interval="1h"}'
        )
    # else default

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
        yield  # Yield control to the block of code using this context manager
    finally:
        logger.debug("Stopping SlateDB NBD server...")
        # Kill the SlateDB process
        process.terminate()
        logger.debug("Waiting for SlateDB NBD server to stop...")
        process.wait()
        logger.debug("SlateDB NBD server stopped.")
