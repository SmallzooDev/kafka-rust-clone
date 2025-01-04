use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::message::{
    KafkaRequest, KafkaResponse, ApiVersionsResponse, ApiVersion, ResponsePayload,
    RequestPayload, DescribeTopicPartitionsResponse
};
use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, MAX_SUPPORTED_VERSION, UNSUPPORTED_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION, UNKNOWN_TOPIC_OR_PARTITION
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
            },
            ApiVersion {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                min_version: DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION,
                max_version: DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION,
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
            DESCRIBE_TOPIC_PARTITIONS_KEY => {
                match &request.payload {
                    RequestPayload::DescribeTopicPartitions(req) => {
                        Ok(KafkaResponse::new(
                            request.header.correlation_id,
                            UNKNOWN_TOPIC_OR_PARTITION,
                            ResponsePayload::DescribeTopicPartitions(
                                DescribeTopicPartitionsResponse::new_unknown_topic(req.topic_name.clone())
                            ),
                        ))
                    }
                    _ => unreachable!(),
                }
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
    match request.header.api_key {
        API_VERSIONS_KEY => {
            if request.header.api_version >= 0 && request.header.api_version <= 4 {
                None
            } else {
                Some(UNSUPPORTED_VERSION)
            }
        }
        DESCRIBE_TOPIC_PARTITIONS_KEY => {
            if request.header.api_version == 0 {
                None
            } else {
                Some(UNSUPPORTED_VERSION)
            }
        }
        _ => Some(UNSUPPORTED_VERSION)
    }
} 