import re
import logging

import subprocess
import sys
import time

from slatedb_nbd_bench.bencher import Bencher


logger = logging.getLogger(__name__)


def bench_sync(zpool: str, *, bencher: Bencher) -> None:
    """
    Benchmarks the sync operation.
    This is a placeholder for the actual benchmarking logic.
    """
    logger.info("Running sync operation...")
    with bencher.bench("sync"):
        subprocess.run(["sudo", "sync"], check=True)
    with bencher.bench("zpool sync"):
        subprocess.run(["sudo", "zpool", "sync", zpool], check=True)
    logger.info("Sync operation completed.")


def bench_trim(zpool: str, *, bencher: Bencher) -> None:
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
    with bencher.bench("wait_for_trim_completion"):
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


def bench_scrub(zpool: str, *, bencher: Bencher) -> None:
    """
    Benchmarks the scrub operation.
    This is a placeholder for the actual benchmarking logic.
    """
    logger.info("Starting ZFS scrub...")
    subprocess.run(["sudo", "zpool", "scrub", zpool], check=True)

    # Poll the status of the scrub operation
    with bencher.bench("wait_for_scrub_completion"):
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

    # We should probably push this to the bencher
    print(f"scrub_status: {match.group(0)}.")

    logger.info("Scrub operation completed.")


def bench_snapshot(dataset: str, *, bencher: Bencher) -> None:
    logger.info("Creating ZFS snapshot...")
    with bencher.bench("zfs_snapshot"):
        subprocess.run(
            ["sudo", "zfs", "snapshot", f"{dataset}@after-kernel"], check=True
        )
    logger.info("ZFS snapshot created successfully.")
