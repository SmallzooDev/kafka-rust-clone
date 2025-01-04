use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::ports::outgoing::metadata_store::MetadataStore;
use crate::Result;
use async_trait::async_trait;
use crate::domain::message::{
    KafkaRequest, KafkaResponse, ApiVersionsResponse, ResponsePayload,
    RequestPayload, DescribeTopicPartitionsResponse, PartitionInfo, KafkaMessage
};
use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, UNSUPPORTED_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_KEY, UNKNOWN_TOPIC_OR_PARTITION
};

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
                        let topic_metadata = self.metadata_store.get_topic_metadata(&req.topic_name).await?;
                        
                        match topic_metadata {
                            Some(metadata) => {
                                // 토픽이 존재하는 경우
                                let partitions = metadata.partitions.iter()
                                    .map(|p| PartitionInfo {
                                        partition_id: p.partition_index,
                                        error_code: 0,
                                    })
                                    .collect();

                                Ok(KafkaResponse::new(
                                    request.header.correlation_id,
                                    0,
                                    ResponsePayload::DescribeTopicPartitions(
                                        DescribeTopicPartitionsResponse {
                                            topic_name: req.topic_name.clone(),
                                            topic_id: metadata.topic_id,
                                            error_code: 0,
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
                                    UNKNOWN_TOPIC_OR_PARTITION,
                                    ResponsePayload::DescribeTopicPartitions(
                                        DescribeTopicPartitionsResponse::new_unknown_topic(req.topic_name.clone())
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
    use crate::domain::message::{RequestHeader, TopicMetadata, PartitionMetadata, DescribeTopicPartitionsRequest, KafkaMessage};

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
        async fn get_topic_metadata(&self, topic_name: &str) -> Result<Option<TopicMetadata>> {
            Ok(self.topics.iter()
                .find(|t| t.name == topic_name)
                .cloned())
        }
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_existing_topic() -> Result<()> {
        let topic_id = [1u8; 16];
        let topic_metadata = TopicMetadata::new(
            "test-topic".to_string(),
            topic_id,
            vec![PartitionMetadata::new(0, 0, vec![0], vec![0])],
        );

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
                assert_eq!(resp.topic_id, topic_id);
                assert_eq!(resp.error_code, 0);
                assert_eq!(resp.partitions.len(), 1);
                assert_eq!(resp.partitions[0].partition_id, 0);
                assert_eq!(resp.partitions[0].error_code, 0);
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
        assert_eq!(response.error_code, UNKNOWN_TOPIC_OR_PARTITION);

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