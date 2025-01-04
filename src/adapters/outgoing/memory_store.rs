use async_trait::async_trait;
use crate::ports::outgoing::message_store::MessageStore;
use crate::domain::message::KafkaMessage;
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
} 