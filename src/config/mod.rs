use std::sync::Arc;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;
use crate::adapters::outgoing::memory_store::MemoryMessageStore;
use crate::application::broker::KafkaBroker;
use crate::ports::incoming::protocol_parser::ProtocolParser;
use crate::ports::outgoing::message_store::MessageStore;
use crate::ports::incoming::message_handler::MessageHandler;

#[cfg(test)]
pub mod test;

pub struct AppConfig {
    pub message_handler: Arc<dyn MessageHandler>,
    pub protocol_parser: Arc<dyn ProtocolParser>,
}

impl AppConfig {
    pub fn new() -> Self {
        // 의존성 생성
        let message_store: Box<dyn MessageStore> = Box::new(MemoryMessageStore::new());
        let broker = Arc::new(KafkaBroker::new(message_store));
        let protocol_parser = Arc::new(KafkaProtocolParser::new());

        Self {
            message_handler: broker,
            protocol_parser,
        }
    }

    pub fn with_custom_components(
        message_handler: Arc<dyn MessageHandler>,
        protocol_parser: Arc<dyn ProtocolParser>,
    ) -> Self {
        Self {
            message_handler,
            protocol_parser,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_creation() {
        let config = AppConfig::new();
        assert!(Arc::strong_count(&config.message_handler) >= 1);
        assert!(Arc::strong_count(&config.protocol_parser) >= 1);
    }

    #[test]
    fn test_app_config_with_custom_components() {
        let config = test::create_test_config();
        assert!(Arc::strong_count(&config.message_handler) >= 1);
        assert!(Arc::strong_count(&config.protocol_parser) >= 1);
    }
} 