use crate::Result;

#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn parse(buffer: &[u8]) -> Result<Self> {
        if buffer.len() < 8 {
            return Err(crate::Error::InvalidRequest);
        }
        
        let api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
        let api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
        let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        
        // client_id는 현재 없음
        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: None,
        })
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub payload: Vec<u8>,
}

pub struct KafkaResponse {
    pub correlation_id: i32,
    pub payload: Vec<u8>,
}

pub struct ProtocolParser;

impl ProtocolParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_request(&self, buffer: &[u8]) -> Result<KafkaRequest> {
        if buffer.len() < 8 {
            return Err(crate::Error::InvalidRequest);
        }
        
        let header = RequestHeader::parse(&buffer[0..])?;
        
        Ok(KafkaRequest {
            header,
            payload: buffer.to_vec(),
        })
    }

    pub fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buffer = Vec::new();
        let total_size = (response.payload.len() + 4) as i32;
        buffer.extend_from_slice(&total_size.to_be_bytes());
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
        assert!(matches!(result, Err(crate::Error::InvalidRequest)));
    }

    #[test]
    fn test_encode_response() {
        let parser = ProtocolParser::new();
        let response = KafkaResponse {
            correlation_id: 7,
            payload: vec![1, 2, 3],
        };

        let encoded = parser.encode_response(response);
        
        assert_eq!(i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]), 7);
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 7);
        assert_eq!(&encoded[8..], &[1, 2, 3]);
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
            payload: vec![1, 2, 3, 4],
        };

        let encoded = parser.encode_response(response);
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 42);
    }
} 