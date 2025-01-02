use crate::domain::error::DomainError;
use crate::domain::message::{KafkaRequest, KafkaResponse, RequestHeader, ResponsePayload, ApiVersionsResponse, ApiVersion, RequestPayload};
use crate::ports::incoming::protocol_parser::ProtocolParser;
use async_trait::async_trait;
use crate::adapters::incoming::protocol::constants::{API_VERSIONS_KEY, MAX_SUPPORTED_VERSION};

pub struct KafkaProtocolParser;

impl KafkaProtocolParser {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolParser for KafkaProtocolParser {
    fn parse_request(&self, buffer: &[u8]) -> Result<KafkaRequest, DomainError> {
        if buffer.len() < 8 {
            return Err(DomainError::InvalidRequest);
        }
        
        let header = RequestHeader::parse(buffer)?;
        
        // API 타입에 따라 적절한 RequestPayload 생성
        let payload = match header.api_key {
            API_VERSIONS_KEY => RequestPayload::ApiVersions,
            _ => return Err(DomainError::InvalidRequest),
        };
        
        Ok(KafkaRequest::new(header, payload))
    }

    fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // 1. 페이로드를 인코딩
        let encoded_payload = match response.payload {
            ResponsePayload::ApiVersions(api_versions) => {
                let versions: Vec<(i16, i16, i16)> = api_versions.api_versions.iter()
                    .map(|v| (v.api_key, v.min_version, v.max_version))
                    .collect();
                crate::adapters::incoming::protocol::utils::encode_api_versions_response(
                    response.error_code,
                    &versions,
                )
            }
        };
        
        // 2. 전체 메시지 크기 계산 (correlation_id 크기(4) + 인코딩된 페이로드 크기)
        let message_size = (4 + encoded_payload.len()) as i32;
        
        // 3. 메시지 구성
        buffer.extend_from_slice(&message_size.to_be_bytes());
        buffer.extend_from_slice(&response.correlation_id.to_be_bytes());
        buffer.extend_from_slice(&encoded_payload);
        
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request() {
        let parser = KafkaProtocolParser::new();
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&[0x00, 0x12]);  // api_key: 18 (API_VERSIONS_KEY)
        test_data.extend_from_slice(&[0x00, 0x04]);  // api_version: 4
        test_data.extend_from_slice(&7i32.to_be_bytes());  // correlation id
        test_data.extend_from_slice(&[1, 2]);  // 추가 데이터 (무시됨)

        let request = parser.parse_request(&test_data).unwrap();
        assert_eq!(request.header.correlation_id, 7);
        assert_eq!(request.header.api_key, API_VERSIONS_KEY);
        assert_eq!(request.header.api_version, 4);
        assert_eq!(request.payload, RequestPayload::ApiVersions);
    }

    #[test]
    fn test_parse_request_with_invalid_size() {
        let parser = KafkaProtocolParser::new();
        let test_data = vec![1, 2, 3];

        let result = parser.parse_request(&test_data);
        assert!(matches!(result, Err(DomainError::InvalidRequest)));
    }

    #[test]
    fn test_encode_response() {
        let parser = KafkaProtocolParser::new();
        let response = KafkaResponse::new(
            7,
            0,
            ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![
                ApiVersion {
                    api_key: API_VERSIONS_KEY,
                    min_version: 0,
                    max_version: MAX_SUPPORTED_VERSION,
                }
            ]))
        );

        let encoded = parser.encode_response(response);
        
        // 1. 전체 메시지 크기 검증
        let expected_size = 4 + // correlation_id
                           2 + // error_code
                           1 + // array length
                           6 + // api version entry (2+2+2)
                           4 + // throttle_time
                           2;  // tagged fields
        assert_eq!(i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]), expected_size);
        
        // 2. correlation_id 검증
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 7);
        
        // 3. payload 검증 (ApiVersions 응답 형식)
        assert_eq!(i16::from_be_bytes([encoded[8], encoded[9]]), 0);  // error_code
        assert_eq!(encoded[10], 2);  // array length (1 + 1)
        assert_eq!(i16::from_be_bytes([encoded[11], encoded[12]]), API_VERSIONS_KEY);
        assert_eq!(i16::from_be_bytes([encoded[13], encoded[14]]), 0);
        assert_eq!(i16::from_be_bytes([encoded[15], encoded[16]]), MAX_SUPPORTED_VERSION);
    }
} 