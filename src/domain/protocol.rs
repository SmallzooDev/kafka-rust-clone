use crate::Result;

pub struct KafkaRequest {
    pub correlation_id: i32,
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
        let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        Ok(KafkaRequest {
            correlation_id,
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
    fn test_parse_request_with_valid_data() {
        let parser = ProtocolParser::new();
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&10i32.to_be_bytes()); // 메시지 크기
        test_data.extend_from_slice(&7i32.to_be_bytes());  // correlation id
        test_data.extend_from_slice(&[1, 2]);  // 페이로드

        let request = parser.parse_request(&test_data).unwrap();
        assert_eq!(request.correlation_id, 7);
        assert_eq!(request.payload, test_data);
    }

    #[test]
    fn test_parse_request_with_invalid_size() {
        let parser = ProtocolParser::new();
        let test_data = vec![1, 2, 3]; // 8바이트보다 작은 데이터

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
        
        // 검증:
        // - 첫 4바이트: 전체 크기 (payload.len + correlation_id 크기)
        // - 다음 4바이트: correlation_id
        // - 나머지: payload
        assert_eq!(i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]), 7); // 4 (correlation_id) + 3 (payload)
        assert_eq!(i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]), 7); // correlation_id
        assert_eq!(&encoded[8..], &[1, 2, 3]); // payload
    }

    #[test]
    fn test_request_response_roundtrip() {
        let parser = ProtocolParser::new();
        let original_correlation_id = 42;
        let original_payload = vec![1, 2, 3, 4];

        // 요청 생성
        let request = KafkaRequest {
            correlation_id: original_correlation_id,
            payload: original_payload.clone(),
        };

        // 응답 생성
        let response = KafkaResponse {
            correlation_id: request.correlation_id,
            payload: request.payload,
        };

        // 응답 인코딩
        let encoded = parser.encode_response(response);

        // 다시 요청으로 파싱
        let decoded_request = parser.parse_request(&encoded).unwrap();

        // 원본 값과 비교
        assert_eq!(decoded_request.correlation_id, original_correlation_id);
        assert!(encoded.ends_with(&original_payload));
    }
} 