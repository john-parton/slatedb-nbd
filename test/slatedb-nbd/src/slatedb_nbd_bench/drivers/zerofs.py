import logging
import os
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def zerofs_background(
    *, automatically_kill: bool = True, wal_enabled: bool | None = None
) -> Iterator[subprocess.Popen]:
    """
    Context manager to run ZeroFS in the background.
    The process is started at the start and killed at the end.
    """
    if wal_enabled is not None:
        raise NotImplementedError(
            "ZeroFS does not support manually enabling or disabling WAL."
        )

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
        subprocess.run(
            ["git", "clone", "git@github.com:Barre/ZeroFS.git", "ZeroFS"], check=False
        )
        os.chdir("ZeroFS")
    else:
        os.chdir("ZeroFS")
        logger.debug("Pulling latest changes for ZeroFS repository...")
        subprocess.run(["git", "pull"], check=True)

    # Actual code is in a subdirectory as of https://github.com/Barre/ZeroFS/commit/c443021c0c5c63b0475ef6c8f8de495f3d395bc6
    os.chdir("zerofs")

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
        time.sleep(30)
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
def setup_plan9():
    # Make directory
    subprocess.run(
        ["sudo", "mkdir", "-p", "/mnt/zerofs_9p_test"],
        check=True,
    )
    subprocess.run(
        [
            "sudo",
            "mount",
            "-t",
            "9p",
            "-o",
            "trans=tcp,port=5564,version=9p2000.L,msize=1048576,cache=mmap,access=user",
            "127.0.0.1",
            "/mnt/zerofs_9p_test",
        ],
        check=True,
    )
    mount = subprocess.run(
        ["mount"], check=True, stdout=subprocess.PIPE, encoding="utf-8"
    )
    print(mount.stdout)

    os.chdir("/mnt/zerofs_9p_test")

    try:
        yield  # Yield control to the block of code using this context manager
    finally:
        os.chdir("/")  # Change the working directory back to root
        # Unmount the directory
        subprocess.run(["sudo", "umount", "/mnt/zerofs_9p_test"], check=True)
        # Remove the directory
        subprocess.run(["sudo", "rmdir", "/mnt/zerofs_9p_test"], check=True)
        logger.info("Plan 9 mount and directory cleaned up.")
