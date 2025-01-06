use async_trait::async_trait;
use crate::adapters::incoming::protocol::messages::KafkaMessage;
use crate::Result;

#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn store_message(&self, message: KafkaMessage) -> Result<()>;
} 