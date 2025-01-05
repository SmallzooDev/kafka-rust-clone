use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
};
use bytes::Buf;

#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersionsResponse {
    pub api_versions: Vec<ApiVersion>,
}

impl ApiVersionsResponse {
    pub fn new(api_versions: Vec<ApiVersion>) -> Self {
        Self { api_versions }
    }

    pub fn default() -> Self {
        Self::new(vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: 4,
            },
            ApiVersion {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                min_version: 0,
                max_version: 0,
            }
        ])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn is_supported_version(&self) -> bool {
        match self.api_key {
            API_VERSIONS_KEY => self.api_version >= 0 && self.api_version <= 4,
            DESCRIBE_TOPIC_PARTITIONS_KEY => self.api_version == 0,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsRequest {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponse {
    pub topic_name: String,
    pub topic_id: [u8; 16],  // UUID as 16 bytes
    pub error_code: i16,     // topic level error code
    pub is_internal: bool,   // is_internal flag
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub error_code: i16,
}

impl DescribeTopicPartitionsResponse {
    pub fn new_unknown_topic(topic_name: String) -> Self {
        Self {
            topic_name,
            topic_id: [0; 16],  // 00000000-0000-0000-0000-000000000000
            error_code: 3,      // UNKNOWN_TOPIC_OR_PARTITION
            is_internal: false, // external topic
            partitions: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestPayload {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
}

#[derive(Debug, Clone)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub payload: RequestPayload,
}

impl KafkaRequest {
    pub fn new(header: RequestHeader, payload: RequestPayload) -> Self {
        Self {
            header,
            payload,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub payload: ResponsePayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponsePayload {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

impl KafkaResponse {
    pub fn new(correlation_id: i32, error_code: i16, payload: ResponsePayload) -> Self {
        Self {
            correlation_id,
            error_code,
            payload,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub correlation_id: i32,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    UnknownTopicId = 100,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicMetadata {
    pub error_code: ErrorCode,
    pub name: String,     // COMPACT_NULLABLE_STRING
    pub topic_id: String, // UUID
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: i32, // A 4-byte integer (bitfield) representing the authorized operations for this topic.
}

#[derive(Debug, Clone, PartialEq)]
pub struct Partition {
    pub error_code: ErrorCode,
    pub partition_index: u32,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub eligible_leader_replicas: Vec<u32>,
    pub last_known_eligible_leader_replicas: Vec<u32>,
    pub off_line_replicas: Vec<u32>,
}

impl Partition {
    pub fn new(
        error_code: ErrorCode,
        partition_index: u32,
        leader_id: u32,
        leader_epoch: u32,
        replicas: Vec<u32>,
        in_sync_replicas: Vec<u32>,
        eligible_leader_replicas: Vec<u32>,
        last_known_eligible_leader_replicas: Vec<u32>,
        off_line_replicas: Vec<u32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            in_sync_replicas,
            eligible_leader_replicas,
            last_known_eligible_leader_replicas,
            off_line_replicas,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::incoming::protocol::constants::{
        MAX_SUPPORTED_VERSION, UNSUPPORTED_VERSION,
        DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION,
        DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION
    };

    #[test]
    fn test_parse_request_header() {
        let header = RequestHeader {
            api_key: 18,
            api_version: 4,
            correlation_id: 1870644833,
            client_id: None,
        };
        assert_eq!(header.api_key, 18);
        assert_eq!(header.api_version, 4);
        assert_eq!(header.correlation_id, 1870644833);
    }

    #[test]
    fn test_parse_request_with_invalid_size() {
        // This test is no longer relevant since parsing is moved to the parser
        // Keeping it as a placeholder for future protocol validation tests
    }

    #[test]
    fn test_kafka_response_creation() {
        let response = KafkaResponse::new(42, UNSUPPORTED_VERSION, ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: MAX_SUPPORTED_VERSION,
            }
        ])));
        assert_eq!(response.correlation_id, 42);
        assert_eq!(response.error_code, UNSUPPORTED_VERSION);
        assert_eq!(response.payload, ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: MAX_SUPPORTED_VERSION,
            }
        ])));
    }

    #[test]
    fn test_api_versions_response_creation() {
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
        let response = ApiVersionsResponse::new(api_versions);
        
        // APIVersions API 검증
        assert_eq!(response.api_versions[0].api_key, API_VERSIONS_KEY);
        assert_eq!(response.api_versions[0].min_version, 0);
        assert_eq!(response.api_versions[0].max_version, MAX_SUPPORTED_VERSION);
        
        // DescribeTopicPartitions API 검증
        assert_eq!(response.api_versions[1].api_key, DESCRIBE_TOPIC_PARTITIONS_KEY);
        assert_eq!(response.api_versions[1].min_version, DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION);
        assert_eq!(response.api_versions[1].max_version, DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION);
    }
} 