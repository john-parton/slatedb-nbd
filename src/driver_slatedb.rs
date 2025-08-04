use slatedb::bytes::Bytes;
use slatedb::config::WriteOptions;
use slatedb::{Db, SlateDBError, WriteBatch};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_nbd::device::NbdDriver;
use tokio_nbd::errors::{OptionReplyError, ProtocolError};
use tokio_nbd::flags::{CommandFlags, ServerFeatures};

// These are settings which must be persistent across restarts
#[derive(Serialize, Deserialize, Debug, Clone)]
enum SlateDbBlockSettings {
    V1 {
        block_size: u64,  // Block size in bytes
        device_size: u64, // Total size of the device in bytes
    },
}

impl SlateDbBlockSettings {
    fn block_size(&self) -> u64 {
        match self {
            SlateDbBlockSettings::V1 { block_size, .. } => *block_size,
        }
    }

    fn device_size(&self) -> u64 {
        match self {
            SlateDbBlockSettings::V1 { device_size, .. } => *device_size,
        }
    }
}

impl Default for SlateDbBlockSettings {
    fn default() -> Self {
        SlateDbBlockSettings::V1 {
            // Block size of 4 KiB
            // Note that the default for most clients are 512 or 1024
            // It is recommended to run the client with `-b 4096` to force compatibility
            block_size: 4096,
            device_size: 10 * 1024 * 1024 * 1024, // 10 GiB,
        }
    }
}

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
    device_size: u64,
    read_only: bool,
}

#[derive(Debug, Error)]
pub enum InitError {
    #[error("Failed to initialize SlateDB: {0}")]
    SlateDBError(#[from] SlateDBError),
    #[error("Failed to serialize or deserialize settings: {0}")]
    JsonError(#[from] serde_json::Error),
}

impl SlateDbDriver {
    const RESERVED_BLOCKS: u64 = 8; // Reserved for metadata, etc.

    pub(crate) async fn try_from_db(db: Db) -> std::result::Result<Self, InitError> {
        // Get zero'th block to determine block size
        // and deserialize metadata
        let settings = match db.get(Self::block_to_key(0)).await? {
            Some(data) => serde_json::from_slice::<SlateDbBlockSettings>(&data)?,
            None => {
                // write out default
                let default_metadata = SlateDbBlockSettings::default();
                db.put(
                    Self::block_to_key(0),
                    &serde_json::to_vec(&default_metadata)?,
                )
                .await?;

                default_metadata
            }
        };

        // Logic to upgrade settings may need to be added here in the future

        Ok(Self {
            db,
            block_size: settings.block_size(),
            device_size: settings.device_size(),
            read_only: false,
        })
    }

    fn block_align(&self, address: u64) -> Result<u64, ProtocolError> {
        if address % self.block_size as u64 != 0 {
            dbg!(
                "Address {} is not aligned to block size {}",
                address,
                self.block_size
            );
            return Err(ProtocolError::CommandNotSupported);
        }
        if address >= (self.device_size + Self::RESERVED_BLOCKS * self.block_size) {
            return Err(ProtocolError::CommandNotSupported);
        }
        Ok(Self::RESERVED_BLOCKS + address / self.block_size)
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
        ServerFeatures::CAN_MULTI_CONN
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

    async fn get_device_size(&self) -> Result<u64, OptionReplyError> {
        // SlateDB does not support multiple devices, so we return the device size
        Ok(self.device_size)
    }

    async fn read(
        &self,
        _flags: CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ProtocolError> {
        // Block logic is copied here, should be refactored later
        let start_block = self.block_align(offset)?;
        let end_block = self.block_align(offset + length as u64)?;
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
                        dbg!(
                            "Data {:?} does not match block size {}",
                            data,
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

        if buff.len() != length as usize {
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
        let start_block = self.block_align(offset)?;

        // Check if the data length is a multiple of BLOCK_SIZE
        self.block_align(data.len() as u64)?;

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
            .map_err(slate_db_error_to_protocol_error)?;

        println!("Flush command completed");
        Ok(())
    }

    async fn trim(
        &self,
        flags: CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<(), ProtocolError> {
        let start_block = self.block_align(offset)?;
        let end_block = self.block_align(offset + length as u64)?;

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
        // Implementation goes here
        let start_block = self.block_align(offset)?;
        let end_block = self.block_align(offset + length as u64)?;

        self.delete_range(start_block, end_block, flags.contains(CommandFlags::FUA))
            .await
    }

    async fn disconnect(&self, _flags: CommandFlags) -> Result<(), ProtocolError> {
        Ok(())
    }

    async fn resize(&self, _flags: CommandFlags, _size: u64) -> Result<(), ProtocolError> {
        // The important thing is to write back out to our metadata block
        // and update the block size
        // We won't support resizing to a smaller size for now
        Err(ProtocolError::CommandNotSupported)
    }

    async fn cache(
        &self,
        _flags: CommandFlags,
        _offset: u64,
        _length: u32,
    ) -> Result<(), ProtocolError> {
        Err(ProtocolError::CommandNotSupported)
    }

    async fn block_status(
        &self,
        flags: tokio_nbd::flags::CommandFlags,
        offset: u64,
        length: u32,
    ) -> Result<(), ProtocolError> {
        Err(ProtocolError::CommandNotSupported)
    }

    fn get_name(&self) -> String {
        "SlateDB NBD Driver".to_string()
    }
}
