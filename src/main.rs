// Put anything that needs to read or write in the 'io' module

mod driver_slatedb;

use crate::driver_slatedb::SlateDbDriver;
use object_store::ObjectStore;
use object_store::aws::S3ConditionalPut;
use slatedb::{Db, Settings};
use std::env;
use std::sync::Arc;
use tokio_nbd::server::NbdServerBuilder;
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize the tracing subscriber for logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("slatedb_nbd=info,tokio_nbd=info"));

    fmt::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    info!("Starting SlateDB NBD server");

    // Need signal handling for graceful shutdown in production code

    let settings = Settings::from_env("SLATEDB_").map_err(|e| {
        error!("Failed to load SlateDB settings: {}", &e);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to load SlateDB settings: {}", e),
        )
    })?;

    info!("Using SlateDB settings: {:?}", settings);

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            // These will be different if you are using real AWS
            .with_allow_http(true)
            .with_endpoint(env::var("AWS_ENDPOINT").unwrap())
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
            .with_bucket_name(env::var("AWS_BUCKET_NAME").unwrap())
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to create object store"),
    );

    let db = Db::builder("/tmp/test_db", object_store)
        .build()
        .await
        .map_err(|e| {
            error!("Failed to create SlateDB: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to create SlateDB")
        })?;

    debug!("SlateDB instance created successfully");
    let device = SlateDbDriver::try_from_db(db).await.map_err(|e| {
        error!("Failed to initialize SlateDB driver: {}", e);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to initialize SlateDB driver",
        )
    })?;

    info!("Initializing NBD server on 127.0.0.0");

    let server = NbdServerBuilder::builder()
        .devices(vec![device])
        .host("127.0.0.0")
        .build();

    info!("Starting NBD server...");
    server.listen().await?;

    info!("NBD server terminated");
    Ok(())
}
