use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
    FETCH_KEY,
};

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
            FETCH_KEY => self.api_version == 16,
            DESCRIBE_TOPIC_PARTITIONS_KEY => self.api_version == 0,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRequest {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsRequest {
    pub topics: Vec<TopicRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchRequest {
    // 현재는 빈 요청만 처리하므로 필드가 필요 없음
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestPayload {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    Fetch(FetchRequest),
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