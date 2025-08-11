# SlateDB NBD Driver

A Network Block Device (NBD) implementation for SlateDB, allowing SlateDB to be used as
a block storage device.

## Overview

SlateDB NBD is a driver that implements the NBD protocol to expose SlateDB as a block
device. This allows SlateDB to be mounted as a standard block device in Linux, which
can then be formatted with filesystems like ZFS, ext4, etc.

The project provides a simple, efficient, and reliable way to use SlateDB's storage
capabilities with standard filesystem tools and applications that expect block device
interfaces.

## Features

- Exposes SlateDB as a standard Linux block device via NBD
- Supports standard block operations (read, write, trim, flush)
- Force Unit Access (FUA) flag support for durable writes
- Zero-write operations for efficient block clearing
- Block-aligned access with proper validation
- Extremely good real world performance (See benchmarks)

## Non-features

- Compression (Use ZFS compression or application-specific compression)
- Encryption (Use ZFS compression or an encrypted overlay fs)
- Tiered storage or complicated caching requirements (Use ZFS tiered storage or tunables. Use SlateDB configuration options.)

## Requirements

- Rust (edition 2021)
- SlateDB
- Tokio for async runtime
- Linux kernel with NBD support

## Building

```bash
cargo build --release
```

## Configuration

Configuration of slatedb-nbd is done through environmental variables. Where possible,
reasonable defaults are implemented, but it is required that you define variables to
describe your S3-compatible storage:

```
AWS_ENDPOINT=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_BUCKET_NAME=
```

## Usage

```bash
# Start the NBD server with SlateDB backend
./target/release/slatedb-nbd

# In another terminal, connect an NBD client to it
sudo nbd-client -b4096 127.0.0.1 10809 /dev/nbd0

# Now you can use /dev/nbd0 as a regular block device
# It is recommended to use a file system which supports compression and encryption
sudo zpool create \
    -o ashift=12 \
    -o encryption=on \
    -o keylocation=file://key_path \
    -o keyformat=passphrase \
    -o compression=zstd \
    -o mountpoint=/mnt/mypool
    mypool /dev/nbd0

# When you're done, unmount the pool
sudo zfs umount mypool
```

## Mounting on Boot / Cleanly unmounting

- TODO: Describe cleanly mounting and unmounting (systemd daemon)

## Implementation Details

The driver uses a fixed block size (4KiB by default) and reserves the first few blocks
for metadata storage. Block 0 stores the device size.

The implementation ensures proper block alignment and validation, handling edge cases
like unaligned reads/writes. It also optimizes for sparse storage by treating
write_zeroes and trim operations identically.

## Benchmarking

The project includes benchmarking tools in the `bench/` directory that can be used to test performance against alternative NBD implementations.

```bash
cd bench/slatedb-nbd
uv sync
uv run slatedb_nbd_bench
```

See the README in the bench directory for further details.

## Testing

Run the test suite to verify functionality:

```bash
cargo test
```

The tests cover various scenarios including aligned/unaligned reads, zero-length
operations, block boundary cases, and more.

Integration tests are run using python, in the `test` directory.

## Future Features

- Support more object storage backends (https://docs.rs/object_store/latest/object_store/): Google Cloud Storage, Azure Blob Storage, HTTP/WebDAV storage
- Easier systemd integration

## Key Differences from ZeroFS

### Performance.

In real-world tests where the object storage is not directly hosted on the test machine,
zfs-on-slatedb via slatedb-nbd is roughly twice as fast as zfs-on-zerofs and roughly ten times faster
than plan9-on-zerofs.

Comprehensive benchmarking tools are included so you can tune to your specific workload.

### Maintainability.

At the time of writing ZeroFS has approximately 15k LoC and slatdb-nbd has only 900
with tokio-nbd adding another 4,000.

The NBD implementation is its own crate: https://crates.io/crates/tokio-nbd

### Architecture

ZeroFS has a complicated architecture, but it can be described at a high level as
recreating a POSIX compliant file system on top of SlateDB and then exposing that via
Plan9 or NFS. The specifics of how things are loaded or cached is complicated, and there
ends up being a large amount of duplication of responsibilities if you run ZFS on top
of the NBD implementation.

In constrast, slatedb-nbd directly maps blocks to SlateDB keys. The magic happens at the
file system level. ZFS is extremely good at compression, encryption, caching, and just
in general optimizing its reads and writes. It's actually so good that _disabling_
SlateDB's in-built write-ahead log and caching actually has no negative impact on
performance.

## License

This project is licensed under the [GPL-2.0-or-later](LICENSE) license.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- SlateDB team for the underlying storage engine
- ZeroFS for banning John Parton from contributions

## Raw benchmark results

For reference, here are some benchmark results

```
========================================
Starting new test run.
{
  "driver": "zerofs",
  "compression": null,
  "encryption": false
}
linux_kernel_source_extraction: 41.917 seconds
recursive_delete: 3.307 seconds
sparse_file_creation: 0.429 seconds
write_big_zeroes: 2.848 seconds
zfs_snapshot: 0.333 seconds
wait_for_trim_completion: 5.049 seconds
wait_for_scrub_completion: 4.035 seconds
scrub_status: scrub repaired 0B in 00:00:05 with 0 errors.
sync: 0.752 seconds
zpool sync: 0.022 seconds
overall_test_duration: 68.133 seconds
Space usage:
3.2GiB	153 objects	zerofs
========================================
Starting new test run.
{
  "driver": "zerofs",
  "compression": "zstd",
  "encryption": false
}
linux_kernel_source_extraction: 32.055 seconds
recursive_delete: 4.429 seconds
sparse_file_creation: 0.632 seconds
write_big_zeroes: 3.396 seconds
zfs_snapshot: 0.355 seconds
wait_for_trim_completion: 4.035 seconds
wait_for_scrub_completion: 6.048 seconds
scrub_status: scrub repaired 0B in 00:00:06 with 0 errors.
sync: 0.732 seconds
zpool sync: 0.032 seconds
overall_test_duration: 60.858 seconds
Space usage:
2.7GiB	151 objects	zerofs
========================================
Starting new test run.
{
  "driver": "zerofs",
  "compression": "zstd",
  "encryption": false,
  "ashift": 12,
  "block_size": 4096
}
linux_kernel_source_extraction: 32.152 seconds
recursive_delete: 3.427 seconds
sparse_file_creation: 0.336 seconds
write_big_zeroes: 2.882 seconds
zfs_snapshot: 0.319 seconds
wait_for_trim_completion: 4.043 seconds
wait_for_scrub_completion: 5.041 seconds
scrub_status: scrub repaired 0B in 00:00:05 with 0 errors.
sync: 4.883 seconds
zpool sync: 0.025 seconds
overall_test_duration: 60.455 seconds
Space usage:
2.1GiB	149 objects	zerofs
========================================
Starting new test run.
{
  "driver": "slatedb-nbd",
  "compression": "zstd-fast",
  "encryption": true,
  "ashift": 12,
  "block_size": 4096
}
linux_kernel_source_extraction: 25.560 seconds
recursive_delete: 1.347 seconds
sparse_file_creation: 2.334 seconds
write_big_zeroes: 0.750 seconds
zfs_snapshot: 0.274 seconds
wait_for_trim_completion: 11.086 seconds
wait_for_scrub_completion: 37.321 seconds
scrub_status: scrub repaired 0B in 00:00:37 with 0 errors.
sync: 0.041 seconds
zpool sync: 0.030 seconds
overall_test_duration: 81.945 seconds
Space usage:
2.7GiB	217 objects	zerofs
========================================
Starting new test run.
{
  "driver": "slatedb-nbd",
  "compression": "zstd",
  "encryption": true,
  "ashift": 12,
  "block_size": 4096
}
linux_kernel_source_extraction: 23.533 seconds
recursive_delete: 1.355 seconds
sparse_file_creation: 0.965 seconds
write_big_zeroes: 0.751 seconds
zfs_snapshot: 0.256 seconds
wait_for_trim_completion: 11.067 seconds
wait_for_scrub_completion: 36.313 seconds
scrub_status: scrub repaired 0B in 00:00:37 with 0 errors.
sync: 0.413 seconds
zpool sync: 0.026 seconds
overall_test_duration: 77.388 seconds
Space usage:
2.6GiB	214 objects	zerofs
========================================
Starting new test run.
Zerofs/Plan 9 baseline test.
linux_kernel_source_extraction: 118.134 seconds
recursive_delete: 41.841 seconds
write_big_zeroes: 3.817 seconds
sync: 2.972 seconds
overall_test_duration: 169.226 seconds
Space usage:
2.0GiB	60 objects	zerofs
```
