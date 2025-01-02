use std::sync::Arc;
use async_trait::async_trait;
use crate::domain::message::{KafkaRequest, KafkaResponse};
use crate::domain::error::DomainError;
use crate::ports::incoming::protocol_parser::ProtocolParser;
use crate::ports::incoming::message_handler::MessageHandler;
use crate::Result;

pub struct MockProtocolParser;

impl MockProtocolParser {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolParser for MockProtocolParser {
    fn parse_request(&self, _buffer: &[u8]) -> std::result::Result<KafkaRequest, DomainError> {
        unimplemented!("Mock implementation")
    }

    fn encode_response(&self, _response: KafkaResponse) -> Vec<u8> {
        unimplemented!("Mock implementation")
    }
}

pub struct MockMessageHandler;

impl MockMessageHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MessageHandler for MockMessageHandler {
    async fn handle_request(&self, _request: KafkaRequest) -> Result<KafkaResponse> {
        unimplemented!("Mock implementation")
    }
}

pub fn create_test_config() -> super::AppConfig {
    super::AppConfig::with_custom_components(
        Arc::new(MockMessageHandler::new()),
        Arc::new(MockProtocolParser::new()),
    )
} 