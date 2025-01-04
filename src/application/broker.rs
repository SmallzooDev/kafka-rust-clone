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
}

#[async_trait]
impl MessageHandler for KafkaBroker {
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse> {
        // 버전 검증
        if !request.header.is_supported_version() {
            return Ok(KafkaResponse::new(
                request.header.correlation_id,
                UNSUPPORTED_VERSION,
                ResponsePayload::ApiVersions(ApiVersionsResponse::default()),
            ));
        }

        // 정상적인 요청 처리
        match request.header.api_key {
            API_VERSIONS_KEY => {
                Ok(KafkaResponse::new(
                    request.header.correlation_id,
                    0,
                    ResponsePayload::ApiVersions(ApiVersionsResponse::default()),
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