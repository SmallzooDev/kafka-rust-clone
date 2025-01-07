use std::sync::Arc;
use std::path::PathBuf;
use crate::application::broker::KafkaBroker;
use crate::adapters::outgoing::disk_store::DiskMessageStore;
use crate::adapters::outgoing::kraft_metadata_store::KraftMetadataStore;
use crate::ports::incoming::message_handler::MessageHandler;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;

pub struct AppConfig {
    pub broker: Arc<dyn MessageHandler>,
    pub protocol_parser: KafkaProtocolParser,
}

impl AppConfig {
    pub fn new(server_properties_path: &str) -> Self {
        let log_dir = PathBuf::from("/tmp/kraft-combined-logs");
        
        // Initialize stores
        let message_store = Box::new(DiskMessageStore::new(log_dir.clone()));
        let metadata_store = Box::new(KraftMetadataStore::new(log_dir));

        // Initialize broker with both stores
        let broker = Arc::new(KafkaBroker::new(message_store, metadata_store));
        let protocol_parser = KafkaProtocolParser::new();

        Self {
            broker,
            protocol_parser,
        }
    }
} 