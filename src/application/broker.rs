use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY,
    FETCH_KEY, UNKNOWN_TOPIC_OR_PARTITION,
    UNSUPPORTED_VERSION,
};
use crate::adapters::incoming::protocol::messages::{
    ApiVersionsResponse, DescribeTopicPartitionsResponse, ErrorCode, FetchResponse,
    KafkaMessage, KafkaRequest, KafkaResponse, PartitionInfo,
    RequestHeader, RequestPayload, ResponsePayload, TopicRequest, TopicResponse,
};
use crate::domain::message::TopicMetadata;
use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::ports::outgoing::metadata_store::MetadataStore;
use crate::application::error::ApplicationError;
use crate::Result;
use async_trait::async_trait;
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
            FETCH_KEY => {
                match &request.payload {
                    RequestPayload::Fetch(fetch_request) => {
                        // 첫 번째 토픽의 topic_id를 사용하여 응답 생성
                        if let Some(first_topic) = fetch_request.topics.first() {
                            // 토픽 ID를 UUID 문자열로 변환
                            let topic_id_hex = hex::encode(first_topic.topic_id);
                            let topic_id = format!(
                                "{}-{}-{}-{}-{}",
                                &topic_id_hex[0..8],
                                &topic_id_hex[8..12],
                                &topic_id_hex[12..16],
                                &topic_id_hex[16..20],
                                &topic_id_hex[20..32]
                            );
                            
                            let topic_metadata = self.metadata_store.get_topic_metadata_by_ids(vec![topic_id]).await?;
                            println!("[BROKER] Got topic metadata: {:?}", topic_metadata);
                            match topic_metadata {
                                Some(metadata_list) => {
                                    if let Some(metadata) = metadata_list.first() {
                                        if metadata.error_code == i16::from(ErrorCode::UnknownTopicOrPartition) {
                                            println!("[BROKER] Topic not found, returning unknown_topic");
                                            Ok(KafkaResponse::new(
                                                request.header.correlation_id,
                                                0,
                                                ResponsePayload::Fetch(FetchResponse::unknown_topic(first_topic.topic_id)),
                                            ))
                                        } else {
                                            println!("[BROKER] Topic found, returning empty_topic");
                                            Ok(KafkaResponse::new(
                                                request.header.correlation_id,
                                                0,
                                                ResponsePayload::Fetch(FetchResponse::empty_topic(first_topic.topic_id)),
                                            ))
                                        }
                                    } else {
                                        println!("[BROKER] No metadata found, returning unknown_topic");
                                        Ok(KafkaResponse::new(
                                            request.header.correlation_id,
                                            0,
                                            ResponsePayload::Fetch(FetchResponse::unknown_topic(first_topic.topic_id)),
                                        ))
                                    }
                                },
                                None => {
                                    println!("[BROKER] No metadata found, returning unknown_topic");
                                    Ok(KafkaResponse::new(
                                        request.header.correlation_id,
                                        0,
                                        ResponsePayload::Fetch(FetchResponse::unknown_topic(first_topic.topic_id)),
                                    ))
                                }
                            }
                        } else {
                            Ok(KafkaResponse::new(
                                request.header.correlation_id,
                                0,
                                ResponsePayload::Fetch(FetchResponse::empty()),
                            ))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            DESCRIBE_TOPIC_PARTITIONS_KEY => {
                match &request.payload {
                    RequestPayload::DescribeTopicPartitions(req) => {
                        // 메타데이터 스토어에서 토픽 정보 조회
                        let topic_names: Vec<String> = req.topics.iter()
                            .map(|t| t.topic_name.clone())
                            .collect();
                        
                        let topic_metadata = self.metadata_store.get_topic_metadata_by_names(topic_names.clone()).await?;
                        
                        let topics = match topic_metadata {
                            Some(metadata_list) => {
                                metadata_list.into_iter().map(|metadata| {
                                    let partitions = metadata.partitions.iter()
                                        .map(|p| PartitionInfo {
                                            partition_id: p.partition_index as i32,
                                            error_code: p.error_code,
                                        })
                                        .collect();

                                    let topic_id_bytes = hex::decode(metadata.topic_id.replace("-", ""))
                                        .unwrap_or(vec![0; 16]);
                                    let mut topic_id = [0u8; 16];
                                    topic_id.copy_from_slice(&topic_id_bytes);

                                    TopicResponse {
                                        topic_name: metadata.name,
                                        topic_id,
                                        error_code: metadata.error_code,
                                        is_internal: false,
                                        partitions,
                                    }
                                }).collect()
                            }
                            None => {
                                // 토픽이 존재하지 않는 경우
                                topic_names.into_iter().map(|topic_name| {
                                    TopicResponse {
                                        topic_name,
                                        topic_id: [0; 16],
                                        error_code: UNKNOWN_TOPIC_OR_PARTITION,
                                        is_internal: false,
                                        partitions: vec![],
                                    }
                                }).collect()
                            }
                        };

                        Ok(KafkaResponse::new(
                            request.header.correlation_id,
                            0,
                            ResponsePayload::DescribeTopicPartitions(
                                DescribeTopicPartitionsResponse { topics }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::incoming::protocol::messages::{DescribeTopicPartitionsRequest, KafkaMessage, RequestHeader};
    use crate::domain::message::TopicMetadata;
    use async_trait::async_trait;

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
        async fn get_topic_metadata_by_names(&self, topic_names: Vec<String>) -> Result<Option<Vec<TopicMetadata>>> {
            let mut result = Vec::new();
            for topic_name in topic_names {
                if let Some(topic) = self.topics.iter().find(|t| t.name == topic_name) {
                    result.push(topic.clone());
                }
            }
            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        }

        async fn get_topic_metadata_by_ids(&self, topic_ids: Vec<String>) -> Result<Option<Vec<TopicMetadata>>> {
            let mut result = Vec::new();
            for topic_id in topic_ids {
                if let Some(topic) = self.topics.iter().find(|t| t.topic_id == topic_id) {
                    result.push(topic.clone());
                }
            }
            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        }
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_existing_topic() -> Result<()> {
        let topic_id = "00000000-0000-0000-0000-000000000001".to_string();
        let topic_metadata = TopicMetadata {
            error_code: i16::from(ErrorCode::None),
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
                    topics: vec![TopicRequest {
                        topic_name: "test-topic".to_string(),
                        partitions: vec![],
                    }],
                }
            ),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topics.len(), 1);
                assert_eq!(resp.topics[0].topic_name, "test-topic");
                let expected_topic_id = hex::decode(topic_id.replace("-", "")).unwrap();
                assert_eq!(&resp.topics[0].topic_id[..], &expected_topic_id[..]);
                assert_eq!(resp.topics[0].error_code, 0);
                assert_eq!(resp.topics[0].partitions.len(), 0);
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
                    topics: vec![TopicRequest {
                        topic_name: "test-topic".to_string(),
                        partitions: vec![],
                    }],
                }
            ),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topics.len(), 1);
                assert_eq!(resp.topics[0].topic_name, "test-topic");
                assert_eq!(resp.topics[0].topic_id, [0u8; 16]);
                assert_eq!(resp.topics[0].error_code, UNKNOWN_TOPIC_OR_PARTITION);
                assert_eq!(resp.topics[0].partitions.len(), 0);
            }
            _ => panic!("Expected DescribeTopicPartitions response"),
        }

        Ok(())
    }
}