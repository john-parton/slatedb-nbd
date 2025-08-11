use std::sync::atomic::{AtomicU64, Ordering};

use slatedb::bytes::Bytes;
use slatedb::config::WriteOptions;
use slatedb::{Db, SlateDBError, WriteBatch};

use thiserror::Error;
use tokio_nbd::device::NbdDriver;
use tokio_nbd::errors::{OptionReplyError, ProtocolError};
use tokio_nbd::flags::{CommandFlags, ServerFeatures};
use tracing::error;

// Constants for defaults
const DEFAULT_BLOCK_SIZE: u64 = 4096; // 4 KiB - Block size is now fixed
const DEFAULT_DEVICE_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB

fn slate_db_error_to_protocol_error(err: SlateDBError) -> ProtocolError {
    match err {
        SlateDBError::IoError(_) => ProtocolError::IO,
        SlateDBError::Unsupported(_) => ProtocolError::CommandNotSupported,
        // Probably reasonable
        _ => ProtocolError::InvalidArgument,
    }
}

pub(crate) struct SlateDbDriver {
    db: Db,
    // These must be read from the metadata block
    block_size: u64,
    device_size: AtomicU64,
    read_only: bool,
}

#[derive(Debug, Error)]
pub enum InitError {
    #[error("Failed to initialize SlateDB: {0}")]
    SlateDBError(#[from] SlateDBError),
    #[error("Failed to read or write to metadata blocks: {0}")]
    MetadataFailure(String),
}

impl SlateDbDriver {
    // Reserved blocks at the start of the device for metadata
    // Block zero is used to store the device size as a u64
    const RESERVED_BLOCKS: u64 = 8;
    const SIZE_BLOCK: u64 = 0;

    async fn _upsert_device_size(db: &Db, desired_size: u64) -> Result<(), InitError> {
        let current_size = match db.get(Self::block_to_key(Self::SIZE_BLOCK)).await? {
            Some(data) if data.len() == 8 => Some(u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            Some(data) if data.is_empty() => None,
            None => None,
            _ => {
                return Err(InitError::MetadataFailure(
                    "Device size metadata is corrupted".to_string(),
                ));
            }
        };

        // A sort of un-idiomatic pattern here
        if let Some(size) = current_size {
            match desired_size.cmp(&size) {
                std::cmp::Ordering::Equal => Ok(()),
                std::cmp::Ordering::Greater => {
                    // We cannot shrink the device size, so we produce an
                    Err(InitError::MetadataFailure(
                        "Cannot shrink device size".to_string(),
                    ))
                }
                std::cmp::Ordering::Less => {
                    // If the current size is greater than the desired size, we can
                    // grow the device by writing the desired size
                    db.put(
                        Self::block_to_key(Self::SIZE_BLOCK),
                        &desired_size.to_le_bytes(),
                    )
                    .await?;
                    // Return Ok to indicate the size was updated
                    Ok(())
                }
            }
        } else {
            // If the current size is None, write out the desired size
            // unconditionally
            db.put(
                Self::block_to_key(Self::SIZE_BLOCK),
                &desired_size.to_le_bytes(),
            )
            .await?;
            Ok(())
        }
    }

    pub(crate) async fn try_from_db(db: Db) -> std::result::Result<Self, InitError> {
        Self::_upsert_device_size(&db, DEFAULT_DEVICE_SIZE).await?;

        Ok(Self {
            db,
            block_size: DEFAULT_BLOCK_SIZE, // Block size is now fixed
            device_size: AtomicU64::new(DEFAULT_DEVICE_SIZE),
            read_only: false,
        })
    }

    // Helper method to check if an address is valid for the device
    fn check_address_valid(&self, address: u64) -> Result<(), ProtocolError> {
        if address % self.block_size != 0 {
            error!(
                "Address {} is not aligned to block size {}",
                address, self.block_size
            );
            return Err(ProtocolError::CommandNotSupported);
        }
        if address
            >= (self.device_size.load(Ordering::Acquire) + Self::RESERVED_BLOCKS * self.block_size)
        {
            error!(
                "Address {} exceeds device size {}",
                address,
                self.device_size.load(Ordering::Acquire)
            );
            return Err(ProtocolError::CommandNotSupported);
        }
        Ok(())
    }

    fn block_to_key(block: u64) -> [u8; 8] {
        (block + Self::RESERVED_BLOCKS).to_le_bytes()
    }

    async fn read_block(&self, block: u64) -> Result<Option<Bytes>, ProtocolError> {
        self.db
            .get(Self::block_to_key(block))
            .await
            .map_err(slate_db_error_to_protocol_error)
    }

    // Because SlateDB is sparse, there is no functional difference between writing
    // zeros and trimming a range
    async fn delete_range(
        &self,
        start_block: u64,
        end_block: u64,
        await_durable: bool,
    ) -> Result<(), ProtocolError> {
        let mut batch = WriteBatch::new();

        for block in start_block..end_block {
            batch.delete(Self::block_to_key(block));
        }

        let write_options = WriteOptions {
            await_durable: await_durable,
        };

        self.db
            .write_with_options(batch, &write_options)
            .await
            .map_err(slate_db_error_to_protocol_error)
    }
}

impl NbdDriver for SlateDbDriver {
    fn get_features(&self) -> ServerFeatures {
        ServerFeatures::SEND_FLUSH
            | ServerFeatures::SEND_FUA
            | ServerFeatures::SEND_TRIM
            | ServerFeatures::SEND_WRITE_ZEROES
            | ServerFeatures::CAN_MULTI_CONN
        // Todo: implement resize. Shouldn't be too bad
    }

    async fn get_read_only(&self) -> Result<bool, OptionReplyError> {
        Ok(self.read_only)
    }

    async fn get_block_size(&self) -> Result<(u32, u32, u32), OptionReplyError> {
        // We *could* support arbitary block sizes with offsets and complicated slicing logic
        // but for now we just return the block size as the min, optimal, and max
        let block_size = self.block_size as u32;
        Ok((block_size, block_size, block_size))
    }

    async fn get_canonical_name(&self) -> Result<String, OptionReplyError> {
        // SlateDB does not support multiple devices, so we return the device name as is
        Ok("SlateDB Device".to_string())
    }

    async fn get_description(&self) -> Result<String, OptionReplyError> {
        // SlateDB does not support descriptions, so we return the device name as the description
        Ok(format!(
            "SlateDB device with block size {} bytes",
            self.block_size
        ))
    }

    fn get_device_size(&self) -> &AtomicU64 {
        &self.device_size
    }

    // async fn get_device_size(&self) -> Result<u64, OptionReplyError> {
    //     // SlateDB does not support multiple devices, so we return the device size
    //     Ok(self.device_size)
    // }

    async fn read(
        &self,
        _flags: CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ProtocolError> {
        // Ensure offset is valid
        self.check_address_valid(offset)?;

        // Calculate block indices
        let start_block = offset / self.block_size + Self::RESERVED_BLOCKS;
        // Calculate end block - need to ensure we have enough blocks to cover the entire length
        let blocks_needed = (length as u64 + self.block_size - 1) / self.block_size; // Ceiling division
        let end_block = start_block + blocks_needed;
        let mut buff = Vec::<u8>::with_capacity(length as usize);

        // println!(
        //     "Handling read command: start_block={}, end_block={}, length={}",
        //     start_block, end_block, length
        // );
        // Consider FuturesOrdered
        for block in start_block..end_block {
            match self.read_block(block).await? {
                Some(data) => {
                    // write
                    if data.len() != self.block_size as usize {
                        error!(
                            "Data length {} does not match block size {}",
                            data.len(),
                            self.block_size
                        );
                        return Err(ProtocolError::InvalidArgument);
                    }
                    buff.extend(data.as_ref());
                }
                // write zeros
                None => {
                    // If the key is not found, we can return an empty block
                    buff.extend(vec![0; self.block_size as usize]);
                }
            }
        }

        // We might have read more data than needed due to block alignment
        // Trim the buffer to match the requested length exactly
        if buff.len() > length as usize {
            // Truncate the buffer to the requested length
            buff.truncate(length as usize);
        } else if buff.len() < length as usize {
            // If we have less data than requested, this is an error condition
            error!(
                "Buffer length {} is less than requested length {}",
                buff.len(),
                length
            );
            return Err(ProtocolError::InvalidArgument);
        }

        Ok(buff)
    }

    async fn write(
        &self,
        flags: CommandFlags,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), ProtocolError> {
        // Ensure offset is valid
        self.check_address_valid(offset)?;

        // Ensure data length is a multiple of block size
        if data.len() % self.block_size as usize != 0 {
            error!(
                "Data length {} is not a multiple of block size {}",
                data.len(),
                self.block_size
            );
            return Err(ProtocolError::InvalidArgument);
        }

        let start_block = offset / self.block_size + Self::RESERVED_BLOCKS;

        let mut batch = WriteBatch::new();

        for (chunk_offset, chunk) in data.chunks(self.block_size as usize).enumerate() {
            let key = Self::block_to_key(start_block + chunk_offset as u64);

            batch.put(key, &chunk);
        }
        let write_options = WriteOptions {
            await_durable: flags.contains(CommandFlags::FUA),
        };

        self.db
            .write_with_options(batch, &write_options)
            .await
            .map_err(slate_db_error_to_protocol_error)
    }

    async fn flush(&self, _flags: CommandFlags) -> Result<(), ProtocolError> {
        self.db
            .flush()
            .await
            .map_err(slate_db_error_to_protocol_error)
    }

    async fn trim(
        &self,
        flags: CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<(), ProtocolError> {
        // Ensure offset is valid
        self.check_address_valid(offset)?;

        // Calculate block indices
        let start_block = offset / self.block_size + Self::RESERVED_BLOCKS;
        // Calculate end block - ensure we have enough blocks to cover the entire length
        let blocks_needed = (length as u64 + self.block_size - 1) / self.block_size; // Ceiling division
        let end_block = start_block + blocks_needed;

        self.delete_range(start_block, end_block, flags.contains(CommandFlags::FUA))
            .await
    }

    async fn write_zeroes(
        &self,
        flags: CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<(), ProtocolError> {
        // TODO Handle fast zero flag
        // Ensure offset is valid
        self.check_address_valid(offset)?;

        // Calculate block indices
        let start_block = offset / self.block_size + Self::RESERVED_BLOCKS;
        // Calculate end block - ensure we have enough blocks to cover the entire length
        let blocks_needed = (length as u64 + self.block_size - 1) / self.block_size; // Ceiling division
        let end_block = start_block + blocks_needed;

        self.delete_range(start_block, end_block, flags.contains(CommandFlags::FUA))
            .await
    }

    async fn disconnect(&self, _flags: CommandFlags) -> Result<(), ProtocolError> {
        self.db
            .close()
            .await
            .map_err(slate_db_error_to_protocol_error)
    }

    fn get_name(&self) -> String {
        "SlateDB NBD Driver".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::driver_slatedb::SlateDbDriver;
    use slatedb::Db;
    use slatedb::object_store::{ObjectStore, memory::InMemory};
    use std::sync::Arc;
    use tokio_nbd::device::NbdDriver;
    use tokio_nbd::flags::CommandFlags;

    // Helper function to create an in-memory SlateDbDriver for testing
    async fn create_test_driver() -> SlateDbDriver {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open("/tmp/test_kv_store", object_store)
            .await
            .expect("failed to create test kv store");
        let driver = SlateDbDriver::try_from_db(kv_store).await.unwrap();
        driver
    }

    // The original bug was related to buffer length not matching requested length
    // This was due to incorrect calculation of end_block when reading data

    #[tokio::test]
    async fn test_read_with_exact_block_size() {
        // This test verifies that reading with a length that's exactly a multiple of the block size works correctly
        let driver = create_test_driver().await;

        // Write data to the device (4096 bytes = 1 block)
        let data = vec![0x42; 4096];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read back the same amount of data
        let read_data = driver.read(CommandFlags::empty(), 0, 4096).await.unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            4096,
            "Buffer length should match requested length"
        );
        assert_eq!(read_data, data, "Read data should match written data");
    }

    #[tokio::test]
    async fn test_read_with_partial_block() {
        // This test verifies that reading with a length that's not a multiple of the block size works correctly
        // The fix should handle this by reading the whole block but returning only the requested amount
        let driver = create_test_driver().await;

        // Write a full block (4096 bytes)
        let data = vec![0x42; 4096];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read only a portion of the block (e.g., 2048 bytes)
        let read_data = driver.read(CommandFlags::empty(), 0, 2048).await.unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            2048,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[0..2048],
            "Read data should match the first half of the written data"
        );
    }

    #[tokio::test]
    async fn test_read_spanning_multiple_blocks() {
        // This test verifies that reading across block boundaries works correctly
        let driver = create_test_driver().await;

        // Write 2 blocks of data (8192 bytes)
        let data = vec![0x42; 8192];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read data spanning both blocks but not the full two blocks (e.g., 6144 bytes)
        let read_data = driver.read(CommandFlags::empty(), 0, 6144).await.unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            6144,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[0..6144],
            "Read data should match the first 6144 bytes of written data"
        );
    }

    #[tokio::test]
    async fn test_read_unaligned_length() {
        // This test verifies that reading an unaligned length works correctly
        let driver = create_test_driver().await;

        // Write 2 blocks of data (8192 bytes)
        let data = vec![0x42; 8192];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read an odd amount of data that doesn't align with block size (e.g., 5000 bytes)
        let read_data = driver.read(CommandFlags::empty(), 0, 5000).await.unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            5000,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[0..5000],
            "Read data should match the first 5000 bytes of written data"
        );
    }

    #[tokio::test]
    async fn test_read_with_offset() {
        // This test verifies that reading with an offset works correctly
        let driver = create_test_driver().await;

        // Write 3 blocks of data (12288 bytes)
        let data = vec![0x42; 12288];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read from the middle of the data (offset of 4096 bytes, which is aligned to a block boundary)
        let read_data = driver
            .read(CommandFlags::empty(), 4096, 4096)
            .await
            .unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            4096,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[4096..8192],
            "Read data should match the second block of written data"
        );
    }

    #[tokio::test]
    async fn test_read_with_offset_and_unaligned_length() {
        // This test specifically targets the original bug by combining offset with unaligned length
        let driver = create_test_driver().await;

        // Write 3 blocks of data (12288 bytes)
        let data = vec![0x42; 12288];
        driver
            .write(CommandFlags::empty(), 0, data.clone())
            .await
            .unwrap();

        // Read from the middle with an unaligned length
        // This would have caused problems in the original implementation
        let read_data = driver
            .read(CommandFlags::empty(), 4096, 3000)
            .await
            .unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            3000,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[4096..7096],
            "Read data should match the expected segment"
        );
    }

    #[tokio::test]
    async fn test_read_large_offset_and_length() {
        // This test verifies reading near the end of the device
        let driver = create_test_driver().await;

        // Write data at a high offset (e.g., 5 MB from the start)
        let offset = 5 * 1024 * 1024; // 5 MB, must be block aligned
        let data = vec![0x42; 8192]; // 2 blocks
        driver
            .write(CommandFlags::empty(), offset, data.clone())
            .await
            .unwrap();

        // Read with an unaligned length
        let read_data = driver
            .read(CommandFlags::empty(), offset, 5000)
            .await
            .unwrap();

        // Verify the data is what we expect
        assert_eq!(
            read_data.len(),
            5000,
            "Buffer length should match requested length"
        );
        assert_eq!(
            read_data,
            data[0..5000],
            "Read data should match the expected segment"
        );
    }
}
