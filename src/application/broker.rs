use crate::ports::incoming::message_handler::{MessageHandler, Response};
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;

pub struct KafkaBroker {
    message_store: Box<dyn MessageStore>,
}

impl KafkaBroker {
    pub fn new(message_store: Box<dyn MessageStore>) -> Self {
        Self { message_store }
    }
}

#[async_trait]
impl MessageHandler for KafkaBroker {
    async fn handle_request(&self, correlation_id: i32, request_data: Vec<u8>) -> Result<Response> {
        // TODO: 실제 요청 처리 로직 구현
        Ok(Response {
            correlation_id,
            payload: Vec::new(), // 임시 빈 응답
        })
    }
} 