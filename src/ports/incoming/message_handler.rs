use async_trait::async_trait;
use crate::Result;
use crate::domain::message::{KafkaRequest, KafkaResponse};

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse>;
} 