use std::sync::Arc;
use std::path::PathBuf;
use crate::application::broker::KafkaBroker;
use crate::adapters::outgoing::memory_store::MemoryMessageStore;
use crate::adapters::outgoing::kraft_metadata_store::KraftMetadataStore;
use crate::ports::incoming::message_handler::MessageHandler;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;

pub struct AppConfig {
    pub broker: Arc<dyn MessageHandler>,
    pub protocol_parser: KafkaProtocolParser,
}

impl AppConfig {
    pub fn new(server_properties_path: &str) -> Self {
        // Initialize stores
        let message_store = Box::new(MemoryMessageStore::new());
        let metadata_store = Box::new(KraftMetadataStore::new(
            PathBuf::from("/tmp/kraft-combined-logs")
        ));

        // Initialize broker with both stores
        let broker = Arc::new(KafkaBroker::new(message_store, metadata_store));
        let protocol_parser = KafkaProtocolParser::new();

        Self {
            broker,
            protocol_parser,
        }
    }
} 