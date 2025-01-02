use async_trait::async_trait;
use crate::Result;

pub struct Response {
    pub correlation_id: i32,
    pub payload: Vec<u8>,
}

#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle_request(&self, correlation_id: i32, request_data: Vec<u8>) -> Result<Response>;
} 