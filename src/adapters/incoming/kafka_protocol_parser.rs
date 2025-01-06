use crate::application::error::ApplicationError;
use crate::adapters::incoming::protocol::messages::{
    ApiVersion, ApiVersionsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
    KafkaRequest, KafkaResponse, RequestHeader, RequestPayload, ResponsePayload, PartitionInfo,
};
use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY, MAX_SUPPORTED_VERSION, UNKNOWN_TOPIC_OR_PARTITION,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone)]
pub struct KafkaProtocolParser;

impl KafkaProtocolParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_request(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError> {
        println!("[REQUEST] Raw bytes: {:02x?}", data);
        let mut buf = Bytes::copy_from_slice(data);
        
        // API Key (2 bytes)
        let api_key = buf.get_u16() as i16;
        println!("[REQUEST] API Key: {}", api_key);
        
        // API Version (2 bytes)
        let api_version = buf.get_u16() as i16;
        println!("[REQUEST] API Version: {}", api_version);
        
        // Correlation ID (4 bytes)
        let correlation_id = buf.get_u32() as i32;
        println!("[REQUEST] Correlation ID: {}", correlation_id);
        
        // Client ID
        let client_id_len = buf.get_u16() as usize;
        println!("[REQUEST] Client ID length: {}", client_id_len);
        let client_id = if client_id_len > 0 {
            let client_id_bytes = buf.copy_to_bytes(client_id_len);
            let client_id = String::from_utf8(client_id_bytes.to_vec())
                .map_err(|_| ApplicationError::Protocol("Invalid client ID encoding".to_string()))?;
            println!("[REQUEST] Client ID: {}", client_id);
            Some(client_id)
        } else {
            println!("[REQUEST] Client ID: None");
            None
        };
        
        buf.get_u8(); // tag buffer
        println!("[REQUEST] Remaining bytes after header: {:02x?}", buf);
        
        let header = RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };
        
        let payload = match api_key {
            API_VERSIONS_KEY => RequestPayload::ApiVersions,
            DESCRIBE_TOPIC_PARTITIONS_KEY => {
                let mut array_length_buf = [0u8; 8];
                let mut pos = 0;
                
                loop {
                    if pos >= buf.len() {
                        return Err(ApplicationError::Protocol("Buffer too short for array length".to_string()));
                    }
                    let byte = buf.get_u8();
                    array_length_buf[pos] = byte;
                    pos += 1;
                    println!("[REQUEST] Array length byte {}: {:02x}", pos, byte);
                    
                    if byte & 0x80 == 0 {
                        break;
                    }
                }
                
                let array_length = decode_varint(&array_length_buf[..pos]) - 1;
                println!("[REQUEST] Array length (decoded): {}", array_length);
                if array_length == 0 {
                    return Err(ApplicationError::Protocol("Invalid array length".to_string()));
                }
                
                let mut name_length_buf = [0u8; 8];
                let mut pos = 0;
                
                loop {
                    if pos >= buf.len() {
                        return Err(ApplicationError::Protocol("Buffer too short for name length".to_string()));
                    }
                    let byte = buf.get_u8();
                    name_length_buf[pos] = byte;
                    pos += 1;
                    println!("[REQUEST] Name length byte {}: {:02x}", pos, byte);
                    
                    if byte & 0x80 == 0 {
                        break;
                    }
                }
                
                let name_length = decode_varint(&name_length_buf[..pos]) - 1;
                println!("[REQUEST] Name length (decoded): {}", name_length);
                println!("[REQUEST] Before topic name parsing, remaining buffer: {:02x?}", buf);
                println!("[REQUEST] Buffer length: {}", buf.len());
                println!("[REQUEST] Buffer contents: {:?}", buf.chunk());
                
                let mut topic_name_buf = vec![0u8; name_length as usize];
                if name_length as usize > buf.len() {
                    println!("[REQUEST] Error: name_length ({}) > remaining buffer length ({})", name_length, buf.len());
                    return Err(ApplicationError::Protocol("Buffer too short for topic name".to_string()));
                }
                let bytes_to_copy = buf.copy_to_bytes(name_length as usize);
                println!("[REQUEST] Bytes to copy: {:02x?}", bytes_to_copy);
                topic_name_buf.copy_from_slice(&bytes_to_copy);
                
                let topic_name = String::from_utf8(topic_name_buf)
                    .map_err(|_| ApplicationError::Protocol("Invalid topic name encoding".to_string()))?;
                println!("[REQUEST] Topic name: {}", topic_name);
                
                buf.get_u8(); // tag buffer
                
                let response_partition_limit = buf.get_u32();
                println!("[REQUEST] Response partition limit: {}", response_partition_limit);
                buf.get_u8(); // cursor
                buf.get_u8(); // tag buffer
                
                RequestPayload::DescribeTopicPartitions(DescribeTopicPartitionsRequest {
                    topic_name,
                    partitions: vec![],
                })
            }
            _ => return Err(ApplicationError::Protocol("Invalid API key".to_string())),
        };
        
        Ok(KafkaRequest::new(header, payload))
    }

    pub fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        
        // correlation_id
        buf.put_i32(response.correlation_id);
        
        match &response.payload {
            ResponsePayload::ApiVersions(api_versions) => {
                buf.put_i16(response.error_code);
                
                // array of api keys
                buf.put_i8((api_versions.api_versions.len() + 1) as i8);
                
                // Write each API version
                for version in &api_versions.api_versions {
                    buf.put_i16(version.api_key);
                    buf.put_i16(version.min_version);
                    buf.put_i16(version.max_version);
                    buf.put_i8(0); // TAG_BUFFER
                }
                
                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER
            }
            ResponsePayload::DescribeTopicPartitions(describe_response) => {
                println!("[RESPONSE] Encoding DescribeTopicPartitions response: {:?}", describe_response);
                
                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER
                
                // topics array length (COMPACT_ARRAY)
                buf.put_i8(2);  // array_length + 1 (1개의 토픽이므로 2)
                
                // topic error code
                buf.put_i16(describe_response.error_code);
                
                // topic name (COMPACT_STRING)
                let topic_name_bytes = describe_response.topic_name.as_bytes();
                buf.put_i8((topic_name_bytes.len() + 1) as i8);
                buf.put_slice(topic_name_bytes);
                
                // topic id (UUID)
                buf.put_slice(&describe_response.topic_id);
                
                // is_internal
                buf.put_i8(describe_response.is_internal as i8);
                
                // partitions array (COMPACT_ARRAY)
                buf.put_i8((describe_response.partitions.len() + 1) as i8);
                println!("[RESPONSE] Encoding {} partitions", describe_response.partitions.len());
                
                // Write each partition
                for partition in &describe_response.partitions {
                    println!("[RESPONSE] Encoding partition: {:?}", partition);
                    buf.put_i16(partition.error_code);  // error code 먼저
                    buf.put_i32(partition.partition_id);  // 그 다음 partition id
                    buf.put_i32(1);  // leader id (1로 고정)
                    buf.put_i32(0);  // leader epoch
                    
                    // replica nodes array
                    buf.put_i8(2);  // array length + 1 (1개의 replica이므로 2)
                    buf.put_i32(1);  // replica node id (1로 고정)
                    
                    // isr nodes array
                    buf.put_i8(2);  // array length + 1 (1개의 isr이므로 2)
                    buf.put_i32(1);  // isr node id (1로 고정)
                    
                    // eligible leader replicas array
                    buf.put_i8(1);  // array length + 1 (0개이므로 1)
                    
                    // last known eligible leader replicas array
                    buf.put_i8(1);  // array length + 1 (0개이므로 1)
                    
                    // offline replicas array
                    buf.put_i8(1);  // array length + 1 (0개이므로 1)
                    
                    buf.put_i8(0);  // TAG_BUFFER for partition
                }
                
                // topic authorized operations
                buf.put_u32(0x00000df8);
                
                buf.put_i8(0);  // TAG_BUFFER for topic
                
                // next_cursor (nullable)
                buf.put_u8(0xff);  // null
                
                buf.put_i8(0);  // TAG_BUFFER for entire response
            }
        }
        
        let total_size = buf.len() as i32;
        let mut final_buf = BytesMut::new();
        final_buf.put_i32(total_size);
        final_buf.put_slice(&buf);
        
        let result = final_buf.to_vec();
        println!("[RESPONSE] Raw bytes: {:02x?}", result);
        result
    }
}

fn decode_varint(buf: &[u8]) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    
    for &byte in buf {
        result |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
        
        if byte & 0x80 == 0 {
            break;
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::incoming::protocol::messages::{
        ApiVersion, ApiVersionsResponse, DescribeTopicPartitionsResponse, PartitionInfo
    };
    use crate::adapters::incoming::protocol::constants::{
        MAX_SUPPORTED_VERSION, UNKNOWN_TOPIC_OR_PARTITION
    };

    #[test]
    fn test_parse_api_versions_request() {
        let mut data = Vec::new();
        
        // Message size (4 bytes)
        let message_size: u32 = 10;  // API Key(2) + API Version(2) + Correlation ID(4) + Client ID length(2)
        data.extend_from_slice(&message_size.to_be_bytes());
        
        // Header
        data.extend_from_slice(&API_VERSIONS_KEY.to_be_bytes());  // API Key
        data.extend_from_slice(&0i16.to_be_bytes());  // API Version
        data.extend_from_slice(&123i32.to_be_bytes());  // Correlation ID
        data.extend_from_slice(&0i16.to_be_bytes());  // Client ID length (0 = null)
        data.push(0); // tag buffer

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data[4..]).unwrap();  // Skip message size

        assert_eq!(request.header.api_key, API_VERSIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, None);
        assert!(matches!(request.payload, RequestPayload::ApiVersions));
    }

    #[test]
    fn test_parse_describe_topic_partitions_request() {
        let mut data = Vec::new();
        
        // Header
        data.extend_from_slice(&DESCRIBE_TOPIC_PARTITIONS_KEY.to_be_bytes());  // API Key
        data.extend_from_slice(&0i16.to_be_bytes());  // API Version
        data.extend_from_slice(&123i32.to_be_bytes());  // Correlation ID
        
        // Client ID
        let client_id = "test-client";
        data.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        data.extend_from_slice(client_id.as_bytes());
        
        // tag buffer after client id
        data.push(0);
        
        // Topics array length (COMPACT_ARRAY)
        data.push(2); // array_length + 1
        
        // Topic name length (COMPACT_STRING)
        let topic_name = "test-topic";
        data.push((topic_name.len() + 1) as u8);
        data.extend_from_slice(topic_name.as_bytes());
        
        // tag buffer after topic name
        data.push(0);
        
        // Response partition limit
        data.extend_from_slice(&1u32.to_be_bytes());
        
        // cursor
        data.push(0);
        
        // tag buffer after cursor
        data.push(0);
        
        // tag buffer at the end
        data.push(0);

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data).unwrap();

        assert_eq!(request.header.api_key, DESCRIBE_TOPIC_PARTITIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, Some("test-client".to_string()));

        match request.payload {
            RequestPayload::DescribeTopicPartitions(req) => {
                assert_eq!(req.topic_name, "test-topic");
                assert_eq!(req.partitions, vec![]);
            }
            _ => panic!("Expected DescribeTopicPartitions payload"),
        }
    }

    #[test]
    fn test_encode_api_versions_response() {
        let response = KafkaResponse::new(
            123,
            0,
            ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![
                ApiVersion {
                    api_key: API_VERSIONS_KEY,
                    min_version: 0,
                    max_version: MAX_SUPPORTED_VERSION,
                }
            ]))
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // Verify size
        let size = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        // Verify correlation ID
        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);
    }

    #[test]
    fn test_encode_describe_topic_partitions_response() {
        let response = KafkaResponse::new(
            123,
            UNKNOWN_TOPIC_OR_PARTITION,
            ResponsePayload::DescribeTopicPartitions(
                DescribeTopicPartitionsResponse::new_unknown_topic("test-topic".to_string())
            )
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // Verify size
        let size = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        // Verify correlation ID
        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);

        // Verify error code
        let error_code = i16::from_be_bytes([encoded[14], encoded[15]]);
        assert_eq!(error_code, UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn test_encode_describe_topic_partitions_response_with_partitions() {
        let response = KafkaResponse::new(
            123,
            0,
            ResponsePayload::DescribeTopicPartitions(
                DescribeTopicPartitionsResponse {
                    topic_name: "test-topic".to_string(),
                    topic_id: [1; 16],  // 모든 바이트가 1인 UUID
                    error_code: 0,
                    is_internal: false,
                    partitions: vec![
                        PartitionInfo {
                            partition_id: 0,
                            error_code: 0,
                        },
                        PartitionInfo {
                            partition_id: 1,
                            error_code: 0,
                        }
                    ],
                }
            )
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // 기본 검증
        let size = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);

        // throttle_time_ms (0)
        assert_eq!(&encoded[8..12], &[0, 0, 0, 0]);
        
        // topics array length (COMPACT_ARRAY) = 2 (1개의 토픽 + 1)
        assert_eq!(encoded[13], 2);

        // topic error code (0)
        assert_eq!(&encoded[14..16], &[0, 0]);

        // topic name length (COMPACT_STRING) = 11 ("test-topic" 길이 + 1)
        assert_eq!(encoded[16], 11);
        assert_eq!(&encoded[17..27], b"test-topic");

        // topic id (UUID) - 모든 바이트가 1
        assert_eq!(&encoded[27..43], &[1; 16]);

        // is_internal
        assert_eq!(encoded[43], 0);

        // partitions array length = 3 (2개의 파티션 + 1)
        assert_eq!(encoded[44], 3);
    }

    #[test]
    fn test_parse_describe_topic_partitions_request_with_multiple_topics() {
        let mut data = Vec::new();
        
        // Header
        data.extend_from_slice(&DESCRIBE_TOPIC_PARTITIONS_KEY.to_be_bytes());  // API Key
        data.extend_from_slice(&0i16.to_be_bytes());  // API Version
        data.extend_from_slice(&123i32.to_be_bytes());  // Correlation ID
        data.extend_from_slice(&0i16.to_be_bytes());  // Client ID length (0 = null)
        data.push(0); // tag buffer
        
        // Topics array length (COMPACT_ARRAY)
        data.push(2); // array_length + 1
        
        // Topic name (COMPACT_STRING)
        let topic_name = "test-topic";
        data.push((topic_name.len() + 1) as u8);
        data.extend_from_slice(topic_name.as_bytes());
        
        // tag buffer after topic name
        data.push(0);
        
        // Response partition limit
        data.extend_from_slice(&1u32.to_be_bytes());
        
        // cursor
        data.push(0);
        
        // tag buffer after cursor
        data.push(0);

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data).unwrap();

        assert_eq!(request.header.api_key, DESCRIBE_TOPIC_PARTITIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, None);

        match request.payload {
            RequestPayload::DescribeTopicPartitions(req) => {
                assert_eq!(req.topic_name, "test-topic");
                assert_eq!(req.partitions, vec![]);
            }
            _ => panic!("Expected DescribeTopicPartitions payload"),
        }
    }
} 