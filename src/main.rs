// Put anything that needs to read or write in the 'io' module

mod driver_slatedb;

use crate::driver_slatedb::SlateDbDriver;
use object_store::ObjectStore;
use object_store::aws::S3ConditionalPut;
use slatedb::{Db, Settings};
use std::env;
use std::sync::Arc;
use tokio_nbd::server::NbdServer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Need signal handling for graceful shutdown in production code

    let settings = Settings::from_env("SLATEDB_").map_err(|e| {
        dbg!("Failed to load SlateDB settings: {}", &e);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to load SlateDB settings: {}", e),
        )
    })?;

    println!("Using SlateDB settings: {:?}", settings);
    println!("SLATEDB_WAL_ENABLED: {}", settings.wal_enabled);

    return Ok(());

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
            dbg!("Failed to create SlateDB: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to create SlateDB")
        })?;

    let device = SlateDbDriver::try_from_db(db).await.map_err(|e| {
        dbg!("Failed to initialize SlateDB driver: {}", e);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to initialize SlateDB driver",
        )
    })?;

    NbdServer::listen(vec![device], "127.0.0.1", None).await?;

    Ok(())
}
