use crate::domain::error::DomainError;

pub const UNSUPPORTED_VERSION: i16 = 35;
pub const MAX_SUPPORTED_VERSION: i16 = 4;
pub const API_VERSIONS_KEY: i16 = 18;

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersion {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.min_version.to_be_bytes());
        buffer.extend_from_slice(&self.max_version.to_be_bytes());
        buffer
    }
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
}

fn encode_unsigned_varint(value: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut val = value;
    
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if val == 0 {
            break;
        }
    }
    bytes
}

impl ApiVersionsResponse {
    pub fn new(error_code: i16) -> Self {
        let api_versions = vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: MAX_SUPPORTED_VERSION,
            }
        ];
        Self { error_code, api_versions }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // 1. error_code (int16)
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        
        // 2. api_versions array length (COMPACT_ARRAY)
        buffer.extend_from_slice(&encode_unsigned_varint(self.api_versions.len() as u32 + 1));
        
        // 3. api_versions array의 각 항목
        for version in &self.api_versions {
            // api_key (int16)
            buffer.extend_from_slice(&version.api_key.to_be_bytes());
            // min_version (int16)
            buffer.extend_from_slice(&version.min_version.to_be_bytes());
            // max_version (int16)
            buffer.extend_from_slice(&version.max_version.to_be_bytes());
        }
        
        // 4. throttle_time_ms (int32)
        buffer.extend_from_slice(&0i32.to_be_bytes());
        
        // 5. tagged fields section
        // - tagged fields section length (UNSIGNED_VARINT)
        buffer.extend_from_slice(&encode_unsigned_varint(1));  // 1 byte for count
        // - tagged fields count (UNSIGNED_VARINT)
        buffer.extend_from_slice(&encode_unsigned_varint(0));
        
        buffer
    }
}

#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn parse(buffer: &[u8]) -> std::result::Result<Self, DomainError> {
        if buffer.len() < 8 {
            return Err(DomainError::InvalidRequest);
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

    pub fn validate_version(&self) -> Option<i16> {
        if self.api_version < 0 || self.api_version > MAX_SUPPORTED_VERSION {
            Some(UNSUPPORTED_VERSION)
        } else {
            None
        }
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub payload: Vec<u8>,
}

pub struct KafkaResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub payload: Vec<u8>,
}

pub struct ProtocolParser;

impl ProtocolParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_request(&self, buffer: &[u8]) -> std::result::Result<KafkaRequest, DomainError> {
        if buffer.len() < 8 {
            return Err(DomainError::InvalidRequest);
        }
        
        let header = RequestHeader::parse(&buffer[0..])?;
        
        Ok(KafkaRequest {
            header,
            payload: buffer.to_vec(),
        })
    }

    pub fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buffer = Vec::new();
        
        // correlation_id(4) + payload
        let message_size = (4 + response.payload.len()) as i32;
        
        buffer.extend_from_slice(&message_size.to_be_bytes());
        buffer.extend_from_slice(&response.correlation_id.to_be_bytes());
        buffer.extend_from_slice(&response.payload);
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_version_validation() {
        // 지원되는 버전 테스트
        let header = RequestHeader {
            api_key: 18,
            api_version: 4,
            correlation_id: 1,
            client_id: None,
        };
        assert_eq!(header.validate_version(), None);

        // 지원되지 않는 버전 테스트 (너무 높은 버전)
        let header = RequestHeader {
            api_key: 18,
            api_version: 5,
            correlation_id: 1,
            client_id: None,
        };
        assert_eq!(header.validate_version(), Some(UNSUPPORTED_VERSION));

        // 지원되지 않는 버전 테스트 (음수 버전)
        let header = RequestHeader {
            api_key: 18,
            api_version: -1,
            correlation_id: 1,
            client_id: None,
        };
        assert_eq!(header.validate_version(), Some(UNSUPPORTED_VERSION));
    }

    #[test]
    fn test_response_with_error_code() {
        let parser = ProtocolParser::new();
        let response = KafkaResponse {
            correlation_id: 42,
            error_code: UNSUPPORTED_VERSION,
            payload: ApiVersionsResponse::new(UNSUPPORTED_VERSION).encode(),
        };

        let encoded = parser.encode_response(response);
        
        // error_code가 ApiVersionsResponse의 payload에 올바르게 인코딩되었는지 확인
        let payload_start = 8;  // correlation_id(4) + size(4)
        assert_eq!(
            i16::from_be_bytes([encoded[payload_start], encoded[payload_start + 1]]), 
            UNSUPPORTED_VERSION
        );
    }

    #[test]
    fn test_parse_request_with_valid_data() {
        let parser = ProtocolParser::new();
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&[0x00, 0x12]);  // api_key: 18
        test_data.extend_from_slice(&[0x00, 0x04]);  // api_version: 4
        test_data.extend_from_slice(&7i32.to_be_bytes());  // correlation id
        test_data.extend_from_slice(&[1, 2]);  // 페이로드

        let request = parser.parse_request(&test_data).unwrap();
        assert_eq!(request.header.correlation_id, 7);
        assert_eq!(request.header.api_key, 18);
        assert_eq!(request.header.api_version, 4);
    }

    #[test]
    fn test_parse_request_with_invalid_size() {
        let parser = ProtocolParser::new();
        let test_data = vec![1, 2, 3];

        let result = parser.parse_request(&test_data);
        assert!(matches!(result, Err(DomainError::InvalidRequest)));
    }

    #[test]
    fn test_encode_response() {
        let parser = ProtocolParser::new();
        let response = KafkaResponse {
            correlation_id: 7,
            error_code: 0,
            payload: vec![1, 2, 3],
        };

        let encoded = parser.encode_response(response);
        
        assert_eq!(i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]), 7); // 4(correlation_id) + 3(payload)
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 7); // correlation_id
        assert_eq!(&encoded[8..], &[1, 2, 3]); // payload
    }

    #[test]
    fn test_request_response_roundtrip() {
        let parser = ProtocolParser::new();
        let mut request_data = Vec::new();
        request_data.extend_from_slice(&[0x00, 0x12]);  // api_key: 18
        request_data.extend_from_slice(&[0x00, 0x04]);  // api_version: 4
        request_data.extend_from_slice(&42i32.to_be_bytes());  // correlation id
        request_data.extend_from_slice(&[1, 2, 3, 4]);  // payload

        let request = parser.parse_request(&request_data).unwrap();
        let response = KafkaResponse {
            correlation_id: request.header.correlation_id,
            error_code: 0,
            payload: vec![1, 2, 3, 4],
        };

        let encoded = parser.encode_response(response);
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 42);
    }

    #[test]
    fn test_api_versions_response_encoding() {
        let response = ApiVersionsResponse::new(0);
        let encoded = response.encode();
        
        // 1. error_code (2 bytes)
        assert_eq!(i16::from_be_bytes([encoded[0], encoded[1]]), 0);
        
        // 2. api_versions array length (COMPACT_ARRAY)
        // COMPACT_ARRAY는 실제 길이 + 1을 인코딩합니다
        assert_eq!(encoded[2], 2);  // 1개의 요소 -> 2로 인코딩
        
        let version = &response.api_versions[0];
        
        // 3. First ApiVersion's fields
        // api_key (int16)
        assert_eq!(i16::from_be_bytes([encoded[3], encoded[4]]), version.api_key);
        
        // min_version (int16)
        assert_eq!(i16::from_be_bytes([encoded[5], encoded[6]]), version.min_version);
        
        // max_version (int16)
        assert_eq!(i16::from_be_bytes([encoded[7], encoded[8]]), version.max_version);
        
        // 4. throttle_time_ms (int32)
        assert_eq!(i32::from_be_bytes([encoded[9], encoded[10], encoded[11], encoded[12]]), 0);
        
        // 5. tagged fields section
        // - tagged fields section length (UNSIGNED_VARINT)
        assert_eq!(encoded[13], 1);  // 1 byte for count
        // - tagged fields count (UNSIGNED_VARINT)
        assert_eq!(encoded[14], 0);
        
        // 6. 전체 길이 확인
        assert_eq!(encoded.len(), 15);
    }

    #[test]
    fn test_encode_unsigned_varint() {
        assert_eq!(encode_unsigned_varint(0), vec![0]);
        assert_eq!(encode_unsigned_varint(1), vec![1]);
        assert_eq!(encode_unsigned_varint(127), vec![127]);
        assert_eq!(encode_unsigned_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_unsigned_varint(300), vec![0xAC, 0x02]);
    }
} 