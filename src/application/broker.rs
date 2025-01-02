use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::message::{
    KafkaRequest, KafkaResponse, ApiVersionsResponse, ApiVersion, ResponsePayload
};
use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, MAX_SUPPORTED_VERSION, UNSUPPORTED_VERSION
};

pub struct KafkaBroker {
    message_store: Box<dyn MessageStore>,
}

impl KafkaBroker {
    pub fn new(message_store: Box<dyn MessageStore>) -> Self {
        Self { message_store }
    }

    fn create_api_versions_response(&self) -> ApiVersionsResponse {
        let api_versions = vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: MAX_SUPPORTED_VERSION,
            }
        ];
        ApiVersionsResponse::new(api_versions)
    }
}

#[async_trait]
impl MessageHandler for KafkaBroker {
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse> {
        // 버전 검증
        if let Some(error_code) = validate_version(&request) {
            return Ok(KafkaResponse::new(
                request.header.correlation_id,
                error_code,
                ResponsePayload::ApiVersions(self.create_api_versions_response()),
            ));
        }

        // 정상적인 요청 처리
        match request.header.api_key {
            API_VERSIONS_KEY => {
                Ok(KafkaResponse::new(
                    request.header.correlation_id,
                    0,
                    ResponsePayload::ApiVersions(self.create_api_versions_response()),
                ))
            }
            _ => Ok(KafkaResponse::new(
                request.header.correlation_id,
                0,
                ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![])),
            ))
        }
    }
}

fn validate_version(request: &KafkaRequest) -> Option<i16> {
    if request.header.api_version < 0 || request.header.api_version > MAX_SUPPORTED_VERSION {
        Some(UNSUPPORTED_VERSION)
    } else {
        None
    }
} 