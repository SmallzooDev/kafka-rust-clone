use async_trait::async_trait;
use crate::domain::message::KafkaMessage;
use crate::Result;

#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn store_message(&self, message: KafkaMessage) -> Result<()>;
} 