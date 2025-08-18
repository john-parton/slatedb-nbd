// Put anything that needs to read or write in the 'io' module

mod driver_slatedb;

use crate::driver_slatedb::SlateDbDriver;
use object_store::ObjectStore;
use object_store::aws::S3ConditionalPut;
use slatedb::{Db, Settings};
use std::sync::Arc;
use tokio_nbd::server::NbdServerBuilder;
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, fmt};

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, env = "SLATEDB_NBD_PORT")]
    port: Option<u16>,

    #[arg(long, env = "AWS_ENDPOINT")]
    s3_endpoint: String,

    #[arg(long, env = "AWS_ALLOW_HTTP", default_value_t = false)]
    s3_allow_http: bool,

    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    s3_access_key_id: String,

    // Pretty sure this should always be an environment variable and you shouldn't
    // be passing sensitive keys are as CLI arguments
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    s3_secret_access_key: String,

    #[arg(long, env = "AWS_BUCKET_NAME")]
    s3_bucket_name: String,
    // TODO Different args for different object storages
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

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

    // TODO Make object store configurable.
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            // These will be different if you are using real AWS
            .with_allow_http(args.s3_allow_http)
            .with_endpoint(args.s3_endpoint)
            .with_access_key_id(args.s3_access_key_id)
            .with_secret_access_key(args.s3_secret_access_key)
            .with_bucket_name(args.s3_bucket_name)
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

    info!("Initializing NBD server on {}", args.host);

    let server = NbdServerBuilder::builder()
        .devices(vec![device])
        .host(&args.host)
        .maybe_port(args.port)
        .build();
    info!("Starting NBD server...");
    server.listen().await?;

    info!("NBD server terminated");
    Ok(())
}
