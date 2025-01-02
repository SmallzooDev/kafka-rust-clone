use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::protocol::{KafkaRequest, KafkaResponse};

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
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse> {
        // 1. 버전 검증
        let error_code = request.header.validate_version().unwrap_or(0);
        
        // 2. 응답 생성
        Ok(KafkaResponse {
            correlation_id: request.header.correlation_id,
            error_code,
            payload: Vec::new(),
        })
    }
} 