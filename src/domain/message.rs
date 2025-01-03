use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, MAX_SUPPORTED_VERSION, UNSUPPORTED_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION,
    DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION
};

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
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn parse(buffer: &[u8]) -> Result<Self, crate::domain::error::DomainError> {
        if buffer.len() < 8 {
            return Err(crate::domain::error::DomainError::InvalidRequest);
        }
        
        let api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
        let api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
        let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        
        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestPayload {
    ApiVersions,
    // 향후 다른 요청 타입들도 여기에 추가
}

#[derive(Debug, Clone)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub payload: RequestPayload,
}

impl KafkaRequest {
    pub fn new(header: RequestHeader, payload: RequestPayload) -> Self {
        Self { header, payload }
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
    // 향후 다른 응답 타입들도 여기에 추가
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::incoming::protocol::constants::{API_VERSIONS_KEY, MAX_SUPPORTED_VERSION, UNSUPPORTED_VERSION};

    #[test]
    fn test_parse_request_header() {
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&[0x00, 0x12]);  // api_key: 18 (ApiVersions)
        test_data.extend_from_slice(&[0x00, 0x04]);  // api_version: 4
        test_data.extend_from_slice(&[0x6f, 0x7f, 0xc6, 0x61]);  // correlation_id: 1870644833

        let header = RequestHeader::parse(&test_data).unwrap();
        assert_eq!(header.api_key, 18);
        assert_eq!(header.api_version, 4);
        assert_eq!(header.correlation_id, 1870644833);
    }

    #[test]
    fn test_parse_request_with_invalid_size() {
        let test_data = vec![1, 2, 3];
        let result = RequestHeader::parse(&test_data);
        assert!(matches!(result, Err(crate::domain::error::DomainError::InvalidRequest)));
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