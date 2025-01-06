use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
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