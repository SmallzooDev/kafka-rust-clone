use async_trait::async_trait;
use crate::adapters::incoming::protocol::messages::KafkaMessage;
use crate::Result;

#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn store_message(&self, message: KafkaMessage) -> Result<()>;
    async fn read_messages(&self, topic_id: &str, partition: i32, offset: i64) -> Result<Option<Vec<u8>>>;
} 