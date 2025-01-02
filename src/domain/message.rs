#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub correlation_id: i32,
    pub payload: Vec<u8>,
} 