use async_trait::async_trait;
use crate::domain::message::{KafkaRequest, KafkaResponse};
use crate::domain::error::DomainError;

#[async_trait]
pub trait ProtocolParser: Send + Sync {
    fn parse_request(&self, buffer: &[u8]) -> Result<KafkaRequest, DomainError>;
    fn encode_response(&self, response: KafkaResponse) -> Vec<u8>;
} 