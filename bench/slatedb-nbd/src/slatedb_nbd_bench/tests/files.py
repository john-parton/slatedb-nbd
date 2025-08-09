import hashlib
import logging
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

import httpx

from slatedb_nbd_bench.bencher import Bencher

logger = logging.getLogger(__name__)


def maybe_download_file(
    *,
    kernel_version: str = "6.16",
    sha256_checksum: str = "1a4be2fe6b5246aa4ac8987a8a4af34c42a8dd7d08b46ab48516bcc1befbcd83",
) -> Path:
    name = f"linux-{kernel_version}.tar.xz"
    tempdir = Path(tempfile.gettempdir())

    maybe_path = tempdir / name

    if not maybe_path.exists():
        logger.info("Downloading Linux kernel source...")
        response = httpx.get(f"https://cdn.kernel.org/pub/linux/kernel/v6.x/{name}")

        response.raise_for_status()

        # Hash is not checked if there happens to be an identically named file
        # in the tempdir already
        file_hash = hashlib.sha256(response.content).hexdigest()
        if file_hash != sha256_checksum:
            raise ValueError(
                f"Checksum mismatch: expected {sha256_checksum}, got {file_hash}"
            )

        with maybe_path.open("wb") as f:
            f.write(response.content)

    return maybe_path


def bench_linux_kernel_source_extraction(
    *,
    bencher: Bencher,
    kernel_version: str = "6.16",
    sha256_checksum: str = "1a4be2fe6b5246aa4ac8987a8a4af34c42a8dd7d08b46ab48516bcc1befbcd83",
) -> None:
    """
    Benchmarks the extraction of the Linux kernel source code.
    This is a placeholder for the actual benchmarking logic.
    """
    name = f"linux-{kernel_version}"

    # Copy from tmp
    shutil.copy(
        maybe_download_file(
            kernel_version=kernel_version,
            sha256_checksum=sha256_checksum,
        ),
        f"{name}.tar.xz",
    )

    # Extract the downloaded tarball
    logger.info("Extracting Linux kernel source...")
    with (
        bencher.bench("linux_kernel_source_extraction"),
        tarfile.open(f"{name}.tar.xz", "r:xz") as tar,
    ):
        logger.info("Extracting Linux kernel source...")
        tar.extractall(path=name)
    logger.info("Linux kernel source extracted.")

    # Remove the downloaded tarball to save space (should be basically instant)
    logger.info("Removing downloaded tarball...")
    with bencher.bench("linux_kernel_source_remove_tarball"):
        Path(f"{name}.tar.xz").unlink()
    logger.info("Downloaded tarball removed.")

    # And then recompress it. Purpose is to bench disk access, so just choose
    # 5 as a reasonable compression level.
    logger.info("Recompressing Linux kernel source...")
    with (
        bencher.bench("linux_kernel_source_recompression"),
        tarfile.open(f"{name}.tar.gz", "w:gz", compresslevel=5) as tar,
    ):
        tar.add(name, arcname=name)
    logger.info("Linux kernel source recompressed.")

    logger.info(f"Deleting extracted Linux kernel source directory {name}...")
    with bencher.bench("linux_kernel_source_deletion"):
        shutil.rmtree(name)
    logger.info(f"Linux kernel source directory {name} deleted.")


def bench_sparse(*, bencher: Bencher) -> None:
    # TODO Test that sparse allocation is ACTUALLY working
    # Consider using: https://pypi.org/project/sparse-file/

    with bencher.bench("sparse_file_creation"):
        subprocess.run(["fallocate", "-l", "1G", "sparse.bin"], check=True)
    # Consider testing reflink here, but it requires a ZFS dataset with bclone
    # enabled and the client-tools installed.


def bench_write_big_zeroes(*, bencher: Bencher) -> None:
    # Consider cross platform support
    with bencher.bench("write_big_zeroes"):
        subprocess.run(
            ["dd", "if=/dev/zero", "of=big_zeroes.bin", "bs=1M", "count=1024"],
            check=True,
        )
