use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::protocol::{
    KafkaRequest, KafkaResponse, ApiVersionsResponse,
    API_VERSIONS_KEY, UNSUPPORTED_VERSION
};

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
        // 버전 검증
        if let Some(error_code) = request.header.validate_version() {
            return Ok(KafkaResponse {
                correlation_id: request.header.correlation_id,
                error_code,
                payload: ApiVersionsResponse::new(error_code).encode(),
            });
        }

        // 정상적인 요청 처리
        match request.header.api_key {
            API_VERSIONS_KEY => Ok(KafkaResponse {
                correlation_id: request.header.correlation_id,
                error_code: 0,
                payload: ApiVersionsResponse::new(0).encode(),
            }),
            _ => Ok(KafkaResponse {
                correlation_id: request.header.correlation_id,
                error_code: UNSUPPORTED_VERSION,
                payload: vec![],
            }),
        }
    }
} 