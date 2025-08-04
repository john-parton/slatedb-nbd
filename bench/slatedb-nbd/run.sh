#!/bin/bash

PORT=10809  # Default NBD port

# Run nbd-client -c /dev/nbd1 and grep to see if there are any non-empty lines
# if so, bail
if nbd-client -c /dev/nbd1 2>/dev/null | grep -q .; then
    echo "ERROR: /dev/nbd1 is already in use by another client!"
    echo "Please disconnect any existing NBD clients before running this script."
    exit 1
fi

# If already running, kill process
if pgrep -f "slatedb_nbd" > /dev/null; then
    echo "ERROR: SlateDB is already running!"

    exit 1
fi

# Clean up prior runs
mcli rm --force --recursive truenas/zerofs

# cd to git root
cd "$(dirname "$0")/../../"

cargo build --release

# Start SlateDB with NBD support in the background
cargo run --profile release &


# Wait for SlateDB NBD server to start
echo "Waiting for SlateDB NBD server to start..."
sleep 10

# Connect to NBD device
echo "Connecting to NBD device..."
sudo nbd-client -b4096 127.0.0.1 $PORT /dev/nbd1 -N "device_$PORT"

# Verify NBD device is available
sudo blockdev --getsize64 /dev/nbd1
sudo fdisk -l /dev/nbd1

# Create ZFS pool directly on NBD block device
echo "Creating ZFS pool on NBD device..."
sudo zpool create -o ashift=12 testpool_slatedb /dev/nbd1

# Verify ZFS pool creation
if ! zpool list testpool_slatedb > /dev/null 2>&1; then
echo "ERROR: ZFS pool creation failed!"
exit 1
fi

# Check pool status
zpool status testpool_slatedb
zpool list testpool_slatedb


# Create a password for encryption
openssl rand -hex 32 > /tmp/testpool_slatedb.key

# Create a ZFS filesystem
# - Encryption
# - Zstd compression
sudo zfs create -o encryption=on -o keyformat=passphrase -o keylocation=file:///tmp/testpool_slatedb.key testpool_slatedb/data

# Set mountpoint
sudo zfs set mountpoint=/mnt/slatedb testpool_slatedb/data


echo "ZFS filesystem created at /mnt/slatedb"

# Set copies=2 for redundancy (allows scrub to detect/repair corruption)
sudo zfs set copies=2 testpool_slatedb/data

# List filesystems
zfs list

# Download Linux kernel source
echo "Downloading Linux kernel 6.15.6..."
sudo chown john:john /mnt/slatedb
cd /mnt/slatedb
wget -q https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.15.6.tar.xz

# Show download size
ls -lh linux-6.15.6.tar.xz

# Reference performance for comparison: ZeroFS is 1 minutes 27 second
# Our run without any compression is 47 seconds
echo "Extracting kernel source..."
time tar -I pixz -xf linux-6.15.6.tar.xz

# Count files to verify extraction
echo "Counting extracted files..."
time find linux-6.15.6 -type f | wc -l

# Recursively delete the extracted kernel source
echo "Deleting extracted kernel source..."
time rm -rf linux-6.15.6

# Create a snapshot
echo "Creating ZFS snapshot..."
time sudo zfs snapshot testpool_slatedb/data@after-kernel

# List snapshots
zfs list -t snapshot

# Show pool I/O statistics
zpool iostat testpool_slatedb 1 5

# Show space usage
zfs list -o name,used,avail,refer,mountpoint

echo "Creating tarball of ZFS data..."
cd /mnt/slatedb
tar -I pigz -cf /tmp/slatedb-backup.tar.gz .

echo "Calculating checksum..."
sha256sum /tmp/slatedb-backup.tar.gz > /tmp/slatedb-backup.sha256
cat /tmp/slatedb-backup.sha256

# Also create checksum of individual files for comparison
# find . -type f -print0 | parallel -0 -j+0 sha256sum | sort > /tmp/file-checksums.txt
# echo "Number of files checksummed: $(wc -l < /tmp/file-checksums.txt)"

echo "Syncing filesystem (sync)..."
time sudo sync
echo "Syncing ZFS pool..."
time sudo zpool sync testpool_slatedb

echo "Running ZFS TRIM..."
time sudo zpool trim testpool_slatedb

Wait for trim to complete
echo "Waiting for TRIM to complete..."
while zpool status testpool_slatedb | grep -q 'trimming' >; do
sleep 2
echo "TRIM in progress..."
done
echo "TRIM completed"
# Show pool status after TRIM (includes duration)
zpool status testpool_slatedb


echo "Starting ZFS scrub..."
sudo zpool scrub testpool_slatedb

Wait for scrub to complete
echo "Waiting for scrub to complete..."
while zpool status testpool_slatedb | grep -q 'scrub in progress'; do
sleep 2
echo "Scrub in progress..."
zpool status testpool_slatedb
done
echo "Scrub completed"

# Show scrub results
zpool status testpool_slatedb

echo "Syncing filesystem (sync)..."
time sudo sync
echo "Syncing ZFS pool..."
time sudo zpool sync testpool_slatedb

sleep 2

echo "Unmounting ZFS filesystem..."
sudo zfs unmount testpool_slatedb/data

echo "Exporting ZFS pool..."
sudo zpool export testpool_slatedb

# Verify pool is exported
if zpool list testpool_slatedb 2> /dev/null; then
echo "ERROR: Pool still imported!"
exit 1
fi
echo "Pool successfully exported"

echo "Disconnecting NBD device..."
sudo nbd-client -d /dev/nbd1

# Wait for NBD to fully disconnect - check if device is actually in use
echo "Waiting for NBD device to disconnect..."
for i in {1..10}; do
# Check if the NBD device is in use by looking at /sys/block/nbd1/size
# When disconnected, this should be 0
if [ ! -e /sys/block/nbd1/size ] || [ "$(cat /sys/block/nbd1/size 2>/dev/null)" = "0" ]; then
    echo "NBD device disconnected successfully"
    break
fi
echo "Waiting for NBD disconnect... attempt $i/10"
sleep 1
done

# Also check using nbd-client -c to see if device is connected
if nbd-client -c /dev/nbd1 2>/dev/null; then
echo "WARNING: NBD device reports as still connected, but continuing..."
else
echo "NBD device confirmed disconnected"
fi

echo "Stopping SlateDB..."
# Find and kill the actual zerofs process (not cargo)
# First try to find the process
SLATEDB_PID=$(pgrep -f "slatedb_nbd" || true)
if [ -n "$SLATEDB_PID" ]; then
echo "Found SlateDB process: $SLATEDB_PID"
sudo kill -TERM $SLATEDB_PID || true
else
echo "No SlateDB process found (may have already stopped)"
fi

# Wait for SlateDB to stop
for i in {1..10}; do
if ! pgrep -f "slatedb_nbd" > /dev/null; then
    echo "SlateDB stopped"
    break
fi
echo "Waiting for SlateDB to stop... attempt $i/10"
sleep 1
done

# Final check if SlateDB stopped
if pgrep -f "slatedb_nbd" > /dev/null; then
echo "WARNING: SlateDB may still be running, forcing kill..."
sudo pkill -KILL -f "slatedb_nbd" || true
sleep 1
fi

# Ensure port is free
echo "Waiting for port $PORT to be free..."
for i in {1..10}; do
if ! nc -z 127.0.0.1 $PORT 2>/dev/null; then
    echo "Port $PORT is free"
    break
fi
echo "Waiting for port to be released... attempt $i/10"
sleep 1
done

# Final check
if nc -z 127.0.0.1 $PORT 2>/dev/null; then
echo "ERROR: Port $PORT still in use after 10 seconds!"
exit 1
fi



# These are durability tests that we should run in another
# script, not here

# echo "Starting SlateDB again..."
# AWS_ALLOW_HTTP=true \
# SLATEDB_CACHE_DIR=/tmp/zerofs-cache \
# SLATEDB_CACHE_SIZE_GB=2 \
# ZEROFS_ENCRYPTION_PASSWORD=secret \
# ZEROFS_NBD_PORTS=10809 \
# ZEROFS_NBD_DEVICE_SIZES_GB=3 \

# cargo run --profile ci s3://zerofs-zfs-test/zfs-test &

# # Wait for SlateDB NBD server to start
# echo "Waiting for SlateDB NBD server to restart..."
# for i in {1..30}; do
# if nc -z 127.0.0.1 10809; then
#     echo "SlateDB NBD server is ready"
#     break
# fi
# sleep 1
# done

# # Verify SlateDB NBD server is running
# if ! nc -z 127.0.0.1 10809; then
# echo "SlateDB NBD server failed to restart"
# exit 1
# fi

# echo "Reconnecting NBD device..."
# sudo nbd-client 127.0.0.1 10809 /dev/nbd1 -N device_10809

# # Verify NBD device is available
# sudo blockdev --getsize64 /dev/nbd1
# sudo fdisk -l /dev/nbd1

# echo "Importing ZFS pool..."
# sudo zpool import testpool

# # Check pool status
# sudo zpool status testpool
# sudo zfs list

# echo "Creating new tarball of restored data..."
# cd /mnt/zfsdata
# sudo tar -I pigz -cf /tmp/zfsdata-restored.tar.gz .

# echo "Calculating checksum of restored data..."
# sudo sha256sum /tmp/zfsdata-restored.tar.gz > /tmp/zfsdata-restored.sha256
# cat /tmp/zfsdata-restored.sha256

# # Compare checksums
# echo "Comparing tarball checksums..."
# ORIGINAL_SUM=$(cut -d' ' -f1 < /tmp/zfsdata-backup.sha256)
# RESTORED_SUM=$(cut -d' ' -f1 < /tmp/zfsdata-restored.sha256)

# if [ "$ORIGINAL_SUM" = "$RESTORED_SUM" ]; then
# echo "SUCCESS: Checksums match! Data integrity verified."
# else
# echo "ERROR: Checksums do not match!"
# echo "Original: $ORIGINAL_SUM"
# echo "Restored: $RESTORED_SUM"
# exit 1
# fi
          
# # Also verify individual file checksums
# echo "Verifying individual file checksums..."
# sudo find . -type f -print0 | parallel -0 -j+0 sha256sum | sort > /tmp/file-checksums-restored.txt

# if diff /tmp/file-checksums.txt /tmp/file-checksums-restored.txt; then
# echo "SUCCESS: All individual file checksums match!"
# else
# echo "ERROR: Individual file checksums differ!"
# exit 1
# fi

# # Show how much data was written to S3
# mcli du truenas/zerofs

# Export and destroy ZFS pool
# zpool export testpool_zerofs || true

# Disconnect NBD device
# nbd-client -d /dev/nbd1 || true

# Kill SlateDB
# pkill -f "cargo run --profile release" || true

