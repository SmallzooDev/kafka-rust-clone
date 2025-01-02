mod adapters;
mod application;
mod config;
mod domain;
mod ports;

use crate::adapters::incoming::tcp_adapter::TcpAdapter;
use crate::config::AppConfig;
use crate::application::{Result, ApplicationError};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::new();
    
    let server = TcpAdapter::new(
        "127.0.0.1:9092",
        config.message_handler,
        config.protocol_parser,
    ).await?;
    
    server.run().await?;
    
    Ok(())
}
