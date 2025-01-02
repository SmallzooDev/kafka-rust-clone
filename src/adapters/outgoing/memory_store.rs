use async_trait::async_trait;
use crate::ports::outgoing::message_store::MessageStore;
use crate::domain::message::KafkaMessage;
use crate::Result;

// 실제 메세지 영속화를 할지는 모르겠지만, out adapter 예시 코드를 위해 임시 작성
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