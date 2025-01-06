use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::ports::outgoing::metadata_store::MetadataStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::message::{
    KafkaRequest, KafkaResponse, ApiVersionsResponse, ResponsePayload,
    RequestPayload, DescribeTopicPartitionsResponse, PartitionInfo, ErrorCode
};
use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, UNSUPPORTED_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_KEY, UNKNOWN_TOPIC_OR_PARTITION
};
use hex;

#[allow(dead_code)]
pub struct KafkaBroker {
    message_store: Box<dyn MessageStore>,
    metadata_store: Box<dyn MetadataStore>,
}

impl KafkaBroker {
    pub fn new(message_store: Box<dyn MessageStore>, metadata_store: Box<dyn MetadataStore>) -> Self {
        Self { 
            message_store,
            metadata_store,
        }
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
                        // 메타데이터 스토어에서 토픽 정보 조회
                        let topic_metadata = self.metadata_store.get_topic_metadata(vec![req.topic_name.clone()]).await?;
                        
                        match topic_metadata {
                            Some(metadata) => {
                                // 토픽이 존재하는 경우
                                let partitions = metadata.partitions.iter()
                                    .map(|p| PartitionInfo {
                                        partition_id: p.partition_index as i32,
                                        error_code: match p.error_code {
                                            ErrorCode::None => 0,
                                            ErrorCode::UnknownTopicOrPartition => 3,
                                            ErrorCode::UnsupportedVersion => 35,
                                            ErrorCode::InvalidRequest => 42,
                                            ErrorCode::UnknownTopicId => 100,
                                        },
                                    })
                                    .collect();

                                let topic_id_bytes = hex::decode(metadata.topic_id.replace("-", ""))
                                    .unwrap_or(vec![0; 16]);
                                let mut topic_id = [0u8; 16];
                                topic_id.copy_from_slice(&topic_id_bytes);

                                Ok(KafkaResponse::new(
                                    request.header.correlation_id,
                                    0,
                                    ResponsePayload::DescribeTopicPartitions(
                                        DescribeTopicPartitionsResponse {
                                            topic_name: req.topic_name.clone(),
                                            topic_id,
                                            error_code: metadata.error_code as i16,
                                            is_internal: false,
                                            partitions,
                                        }
                                    ),
                                ))
                            }
                            None => {
                                // 토픽이 존재하지 않는 경우
                                Ok(KafkaResponse::new(
                                    request.header.correlation_id,
                                    0,
                                    ResponsePayload::DescribeTopicPartitions(
                                        DescribeTopicPartitionsResponse {
                                            topic_name: req.topic_name.clone(),
                                            topic_id: [0; 16],
                                            error_code: UNKNOWN_TOPIC_OR_PARTITION,
                                            is_internal: false,
                                            partitions: vec![],
                                        }
                                    ),
                                ))
                            }
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use async_trait::async_trait;
    use crate::domain::message::{RequestHeader, TopicMetadata, DescribeTopicPartitionsRequest, KafkaMessage};

    struct MockMessageStore;
    #[async_trait]
    impl MessageStore for MockMessageStore {
        async fn store_message(&self, message: KafkaMessage) -> Result<()> {
            Ok(())
        }
    }

    struct MockMetadataStore {
        topics: Vec<TopicMetadata>,
    }

    impl MockMetadataStore {
        fn new(topics: Vec<TopicMetadata>) -> Self {
            Self { topics }
        }
    }

    #[async_trait]
    impl MetadataStore for MockMetadataStore {
        async fn get_topic_metadata(&self, topic_names: Vec<String>) -> Result<Option<TopicMetadata>> {
            Ok(self.topics.iter()
                .find(|t| topic_names.contains(&t.name))
                .cloned())
        }
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_existing_topic() -> Result<()> {
        let topic_id = "00000000-0000-0000-0000-000000000001".to_string();
        let topic_metadata = TopicMetadata {
            error_code: crate::domain::message::ErrorCode::None,
            name: "test-topic".to_string(),
            topic_id: topic_id.clone(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0x0DF,
        };

        let broker = KafkaBroker::new(
            Box::new(MockMessageStore),
            Box::new(MockMetadataStore::new(vec![topic_metadata])),
        );

        let request = KafkaRequest::new(
            RequestHeader {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                api_version: 0,
                correlation_id: 123,
                client_id: None,
            },
            RequestPayload::DescribeTopicPartitions(
                DescribeTopicPartitionsRequest {
                    topic_name: "test-topic".to_string(),
                    partitions: vec![],
                }
            ),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topic_name, "test-topic");
                let expected_topic_id = hex::decode(topic_id.replace("-", "")).unwrap();
                assert_eq!(&resp.topic_id[..], &expected_topic_id[..]);
                assert_eq!(resp.error_code, 0);
                assert_eq!(resp.partitions.len(), 0);
            }
            _ => panic!("Expected DescribeTopicPartitions response"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_nonexistent_topic() -> Result<()> {
        let broker = KafkaBroker::new(
            Box::new(MockMessageStore),
            Box::new(MockMetadataStore::new(vec![])),
        );

        let request = KafkaRequest::new(
            RequestHeader {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                api_version: 0,
                correlation_id: 123,
                client_id: None,
            },
            RequestPayload::DescribeTopicPartitions(
                DescribeTopicPartitionsRequest {
                    topic_name: "test-topic".to_string(),
                    partitions: vec![],
                }
            ),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topic_name, "test-topic");
                assert_eq!(resp.topic_id, [0u8; 16]);
                assert_eq!(resp.error_code, UNKNOWN_TOPIC_OR_PARTITION);
                assert_eq!(resp.partitions.len(), 0);
            }
            _ => panic!("Expected DescribeTopicPartitions response"),
        }

        Ok(())
    }
}