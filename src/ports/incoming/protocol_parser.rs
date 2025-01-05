use crate::application::error::ApplicationError;
use crate::domain::message::{KafkaRequest, KafkaResponse};
use async_trait::async_trait;

#[async_trait]
pub trait ProtocolParser: Send + Sync {
    fn parse_request(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError>;
    fn encode_response(&self, response: KafkaResponse) -> Vec<u8>;
} 