import logging

import subprocess

from slatedb_nbd_bench.bencher import Bencher


logger = logging.getLogger(__name__)


def bench_linux_kernel_source_extraction(*, bencher: Bencher) -> None:
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
    with Bencher.bench("linux_kernel_source_extraction"):
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


def bench_recursive_delete(*, bencher: Bencher) -> None:
    """
    Benchmarks the recursive deletion of the Linux kernel source directory.
    This is a placeholder for the actual benchmarking logic.
    """
    kernel_version = "6.15.6"
    kernel_dir = f"linux-{kernel_version}"

    logger.info(f"Deleting extracted Linux kernel source directory {kernel_dir}...")
    with bencher.bench("recursive_delete"):
        subprocess.run(["rm", "-rf", kernel_dir], check=True)
    logger.info(f"Linux kernel source directory {kernel_dir} deleted.")


def bench_sparse(*, bencher: Bencher) -> None:
    # TODO Test that sparse allocation is ACTUALLY working

    with bencher.bench("sparse_file_creation"):
        subprocess.run(["fallocate", "-l", "1G", "sparse.bin"], check=True)
    # Consider testing reflink here, but it requires a ZFS dataset with bclone
    # enabled and the client-tools installed.


def bench_write_big_zeroes(*, bencher: Bencher) -> None:
    with bencher.bench("write_big_zeroes"):
        subprocess.run(
            ["dd", "if=/dev/zero", "of=big_zeroes.bin", "bs=1M", "count=1024"],
            check=True,
        )
