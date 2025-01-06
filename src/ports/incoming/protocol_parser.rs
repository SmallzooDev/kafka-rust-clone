use crate::Result;
use crate::adapters::incoming::protocol::messages::{KafkaRequest, KafkaResponse};
use crate::application::error::ApplicationError;
use async_trait::async_trait;

#[async_trait]
pub trait ProtocolParser: Send + Sync {
    fn parse_request(&self, data: &[u8]) -> std::result::Result<KafkaRequest, ApplicationError>;
    fn encode_response(&self, response: KafkaResponse) -> Vec<u8>;
} 