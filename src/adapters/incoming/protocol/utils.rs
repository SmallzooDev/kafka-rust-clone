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
    
    // 1. error_code를 big-endian으로 인코딩함
    buffer.extend_from_slice(&error_code.to_be_bytes());
    
    // 2. api_versions 배열을 COMPACT_ARRAY 형식으로 인코딩함
    buffer.extend_from_slice(&encode_unsigned_varint(api_versions.len() as u32 + 1));
    
    // 3. 각 ApiVersion 항목을 인코딩함
    for &(api_key, min_version, max_version) in api_versions {
        buffer.extend_from_slice(&api_key.to_be_bytes());
        buffer.extend_from_slice(&min_version.to_be_bytes());
        buffer.extend_from_slice(&max_version.to_be_bytes());
    }
    
    // 4. throttle_time_ms를 0으로 설정하여 인코딩함
    buffer.extend_from_slice(&0i32.to_be_bytes());
    
    // 5. tagged_fields 섹션을 빈 상태로 인코딩함
    buffer.extend_from_slice(&encode_unsigned_varint(1));  // 섹션 길이 = 1
    buffer.extend_from_slice(&encode_unsigned_varint(0));  // 태그 필드 개수 = 0
    
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
        
        // 1. error_code (2바이트) 검증
        assert_eq!(i16::from_be_bytes([encoded[0], encoded[1]]), 0);
        
        // 2. api_versions 배열 길이 (COMPACT_ARRAY) 검증
        assert_eq!(encoded[2], 2);  // 1개의 요소 -> 2로 인코딩됨
        
        // 3. ApiVersion 항목 필드 검증
        // api_key (int16)
        assert_eq!(i16::from_be_bytes([encoded[3], encoded[4]]), API_VERSIONS_KEY);
        // min_version (int16)
        assert_eq!(i16::from_be_bytes([encoded[5], encoded[6]]), 0);
        // max_version (int16)
        assert_eq!(i16::from_be_bytes([encoded[7], encoded[8]]), MAX_SUPPORTED_VERSION);
        
        // 4. throttle_time_ms (int32) 검증
        assert_eq!(i32::from_be_bytes([encoded[9], encoded[10], encoded[11], encoded[12]]), 0);
        
        // 5. tagged_fields 섹션 검증
        assert_eq!(encoded[13], 1);  // 섹션 길이
        assert_eq!(encoded[14], 0);  // 태그 필드 개수
    }
} 