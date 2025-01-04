mod adapters;
mod application;
mod config;
mod domain;
mod ports;

use crate::application::Result;
use std::sync::Arc;
use crate::config::AppConfig;
use crate::adapters::incoming::tcp_adapter::TcpAdapter;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9092";
    let config = AppConfig::new("server.properties");
    let protocol_parser = Arc::new(KafkaProtocolParser::new());

    let adapter = TcpAdapter::new(
        addr,
        config.broker,
        protocol_parser,
    ).await?;

    adapter.run().await?;

    Ok(())
}
