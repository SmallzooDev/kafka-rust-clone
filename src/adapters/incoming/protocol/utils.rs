pub fn encode_unsigned_varint(value: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut val = value;
    
    loop {
        let mut byte = (val & 0x7f) as u8;  // 하위 7비트만 사용함
        val >>= 7;  // 다음 7비트를 처리하기 위해 우측으로 시프트
        if val != 0 {
            byte |= 0x80;  // 다음 바이트가 있음을 표시하기 위해 MSB를 1로 설정
        }
        bytes.push(byte);
        if val == 0 {
            break;
        }
    }
    bytes
}

pub fn encode_api_versions_response(error_code: i16, api_versions: &[(i16, i16, i16)]) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    // 1. ErrorCode (int16)
    buffer.extend_from_slice(&error_code.to_be_bytes());
    
    // 2. ApiKeys (COMPACT_ARRAY of ApiVersion)
    buffer.extend_from_slice(&encode_unsigned_varint((api_versions.len() + 1) as u32));
    
    // 각 ApiVersion 항목
    for &(api_key, min_version, max_version) in api_versions {
        // ApiKey (int16)
        buffer.extend_from_slice(&api_key.to_be_bytes());
        // MinVersion (int16)
        buffer.extend_from_slice(&min_version.to_be_bytes());
        // MaxVersion (int16)
        buffer.extend_from_slice(&max_version.to_be_bytes());
        // TagBuffer for ApiVersion
        buffer.extend_from_slice(&encode_unsigned_varint(0));
    }
    
    // 3. ThrottleTimeMs (int32) - 정확히 4바이트로 인코딩
    let throttle_time_ms: i32 = 0;
    buffer.extend_from_slice(&throttle_time_ms.to_be_bytes());
    
    // 4. Final TagBuffer
    buffer.extend_from_slice(&encode_unsigned_varint(0));
    
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::incoming::protocol::constants::{API_VERSIONS_KEY, MAX_SUPPORTED_VERSION};

    #[test]
    fn test_encode_unsigned_varint() {
        assert_eq!(encode_unsigned_varint(0), vec![0]);
        assert_eq!(encode_unsigned_varint(1), vec![1]);
        assert_eq!(encode_unsigned_varint(127), vec![127]);
        assert_eq!(encode_unsigned_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_unsigned_varint(300), vec![0xAC, 0x02]);
    }

    #[test]
    fn test_encode_api_versions_response() {
        let api_versions = vec![(API_VERSIONS_KEY, 0, MAX_SUPPORTED_VERSION)];
        let encoded = encode_api_versions_response(0, &api_versions);
        
        let mut index = 0;
        
        // 1. ErrorCode (2바이트)
        assert_eq!(i16::from_be_bytes([encoded[index], encoded[index+1]]), 0);
        index += 2;
        
        // 2. ApiKeys 배열 길이 (COMPACT_ARRAY)
        assert_eq!(encoded[index], 2);  // 실제 길이 + 1 = 1 + 1 = 2
        index += 1;
        
        // 3. ApiVersion 항목
        assert_eq!(i16::from_be_bytes([encoded[index], encoded[index+1]]), API_VERSIONS_KEY);
        index += 2;
        assert_eq!(i16::from_be_bytes([encoded[index], encoded[index+1]]), 0);
        index += 2;
        assert_eq!(i16::from_be_bytes([encoded[index], encoded[index+1]]), MAX_SUPPORTED_VERSION);
        index += 2;
        
        // ApiVersion의 TagBuffer
        assert_eq!(encoded[index], 0);
        index += 1;
        
        // 4. ThrottleTimeMs
        assert_eq!(i32::from_be_bytes([encoded[index], encoded[index+1], encoded[index+2], encoded[index+3]]), 0);
        index += 4;
        
        // 5. Final TagBuffer
        assert_eq!(encoded[index], 0);
    }
} 