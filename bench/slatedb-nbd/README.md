Python scripts for benchmarking slatedb-ndb as well as alternative drivers.

# Setup

Before beginning, you will need to set up a semi-permanent MinIO installation.
You can create it on the same machine that you are testing, but that might not
reflect the real world performance once deployed.

Alternatively, you can run the benchmark directly against the object storage
that you plan to use in production, however, you will likely incur significant
charges, as the benchmarks are rather intensive.

All of the benchmark results included here are run with the following set up:

- MinIO installation on a local server connected to a 10GB switch with generous resources
- All tests are run on a medium-end laptop connected to the network via gigabit ethernet

The intent is to emulate a similar environment to a cloud deployment with a regional
bucket.

## Precautions

The tests run involve creating and destroying ZFS pools. It's unlikely that there will
be any data loss, as all test pools includes an underscore followed by 4 random hex
digits.

However, the device `/dev/nbd6` is unmounted and mounted many times. Ensure you do not
have anything using that device.

## Install Depenencies

The best way to get started is by install [uv](https://github.com/astral-sh/uv)

There are required system tools, which depend on the specific tests that need to be run.

## Run benchmarks

- `uv sync`
- `uv run slatedb_nbd_bench`
