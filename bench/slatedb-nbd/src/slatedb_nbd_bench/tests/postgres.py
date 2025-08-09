# These tests do not work at the time of writing

import logging
import os
import secrets
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager

from slatedb_nbd_bench.bencher import Bencher

logger = logging.getLogger(__name__)


@contextmanager
def postgres_container(
    mountpoint: str,
    *,
    postgres_version: str = "17.5",
    bencher: Bencher,
) -> Iterator[subprocess.Popen]:
    """Context manager to run a PostgreSQL container.
    The container is started at the start and stopped at the end.
    """
    suffix = secrets.token_hex(4)

    logger.info("Pulling PostgreSQL Docker image...")
    subprocess.run(
        ["docker", "pull", f"postgres:{postgres_version}"],
        check=True,
    )

    # Remove a container if it exists
    subprocess.run(
        ["docker", "rm", "-f", f"postgres_init_{suffix}"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # host all all 172.18.0.0/16 md5

    with bencher.bench("postgres_container_initialization"):
        logger.info("Initializing PostgreSQL test container...")
        subprocess.run(
            [
                "docker",
                "run",
                "--name",
                f"postgres_init_{suffix}",
                "-e",
                "POSTGRES_PASSWORD=secret",
                "-e",
                'POSTGRES_INITDB_ARGS="--auth=scram-sha-256"',
                "-v",
                f"{mountpoint}/postgres:/var/lib/postgresql/data",
                f"postgres:{postgres_version}",
                # This is actually not valid. It will cause the container to exit after initialization
                "-c",
                "exit 0",
            ],
            check=False,
        )

    # Remove the container after initialization
    logger.info("Removing PostgreSQL old serve container...")
    subprocess.run(
        ["docker", "rm", "-f", f"postgres_serve_{suffix}"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Start the container listening on port 5434
    logger.info("Starting PostgreSQL test container...")
    docker = subprocess.Popen(
        [
            "docker",
            "run",
            "--name",
            f"postgres_serve_{suffix}",
            # Environmental variables not explicitly required here, only for init
            "-e",
            "POSTGRES_PASSWORD=secret",
            "-p",
            "5434:5432",
            "-v",
            f"{mountpoint}/postgres:/var/lib/postgresql/data",
            f"postgres:{postgres_version}",
        ]
    )
    try:
        # Wait for PostgreSQL to start
        logger.info("Waiting for PostgreSQL to start...")
        time.sleep(10)
        yield docker  # Yield control to the block of code using this context manager
    finally:
        # Stop the PostgreSQL container
        logger.info("Stopping PostgreSQL container...")
        subprocess.run(["docker", "stop", f"postgres_serve_{suffix}"], check=False)
        # Wait for the container to stop

        logger.debug("Waiting for PostgreSQL to stop...")
        # Kill the ZeroFS process
        docker.terminate()
        logger.debug("Waiting for PostgreSQL container to stop...")
        docker.wait()
        logger.info("PostgreSQL container stopped.")


def bench_postgres(
    mountpoint: str, *, postgres_version: str = "17.5", bencher: Bencher
) -> None:
    """
    Benchmarks PostgreSQL operations.
    This is a placeholder for the actual benchmarking logic.
    """
    pgbench_env = os.environ.copy()
    pgbench_env["PGPASSWORD"] = "secret"

    with postgres_container(mountpoint, postgres_version):
        # Init test database
        with bencher.bench("pgbench_initialization"):
            pgbench = subprocess.run(
                [
                    "pgbench",
                    "-U",
                    "postgres",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    "5434",
                    "-i",
                    "postgres",
                ],
                check=False,
                env=pgbench_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
            )

        print("PostgreSQL database initialized.")
        print("Return code:", pgbench.returncode)
        print("STDOUT")
        print(pgbench.stdout)
        print("STDERR")
        print(pgbench.stderr)

        with bencher.bench("pgbench_run"):
            pgbench = subprocess.run(
                [
                    "pgbench",
                    "-U",
                    "postgres",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    "5434",
                    "-i",
                    "postgres",
                ],
                check=False,
                env=pgbench_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
            )
            print("PostgreSQL pgbench run completed.")
            print("Return code:", pgbench.returncode)
            print("STDOUT")
            print(pgbench.stdout)
            print("STDERR")
            print(pgbench.stderr)
