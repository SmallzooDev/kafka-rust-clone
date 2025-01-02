use std::sync::Arc;
use kafka_starter::adapters::incoming::tcp_adapter::TcpAdapter;
use kafka_starter::adapters::outgoing::memory_store::MemoryMessageStore;
use kafka_starter::application::broker::KafkaBroker;
use kafka_starter::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let message_store = Box::new(MemoryMessageStore::new());
    let broker = Arc::new(KafkaBroker::new(message_store));
    
    let server = TcpAdapter::new(
        "127.0.0.1:9092",
        broker.clone(),
    )?;
    server.run().await
}
