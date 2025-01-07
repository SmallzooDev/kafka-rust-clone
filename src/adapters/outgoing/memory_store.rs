use async_trait::async_trait;
use crate::ports::outgoing::message_store::MessageStore;
use crate::adapters::incoming::protocol::messages::KafkaMessage;
use crate::Result;

pub struct MemoryMessageStore {
}

impl MemoryMessageStore {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MessageStore for MemoryMessageStore {
    async fn store_message(&self, _message: KafkaMessage) -> Result<()> {
        Ok(())
    }

    async fn read_messages(&self, _topic_id: &str, _partition: i32, _offset: i64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
} 