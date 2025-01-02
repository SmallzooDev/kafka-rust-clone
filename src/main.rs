use kafka_starter::adapters::incoming::tcp_adapter::TcpAdapter;
use kafka_starter::config::AppConfig;
use kafka_starter::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::new();
    
    let server = TcpAdapter::new(
        "127.0.0.1:9092",
        config.message_handler,
        config.protocol_parser,
    )?;
    
    server.run().await
}
