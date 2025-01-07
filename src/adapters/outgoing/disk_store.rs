use crate::adapters::incoming::protocol::messages::KafkaMessage;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use hex;
use std::path::PathBuf;
use tokio::fs::{self};

pub struct DiskMessageStore {
    log_dir: PathBuf,
}

impl DiskMessageStore {
    pub fn new(log_dir: PathBuf) -> Self {
        Self { log_dir }
    }

    fn get_topic_log_path(&self, topic_name: &str, partition: i32) -> PathBuf {
        self.log_dir.join(format!("{}-{}", topic_name, partition)).join("00000000000000000000.log")
    }
}

#[async_trait]
impl MessageStore for DiskMessageStore {
    async fn store_message(&self, message: KafkaMessage) -> Result<()> {
        // TODO: Implement message storing logic
        Ok(())
    }

    async fn read_messages(&self, topic_id: &str, partition: i32, offset: i64) -> Result<Option<Vec<u8>>> {
        let path = self.get_topic_log_path(topic_id, partition);
        println!("[DEBUG] Looking for file at path: {:?}", path);
        
        match fs::read(&path).await {
            Ok(content) => {
                println!("[DEBUG] Successfully read {} bytes from file", content.len());
                println!("[DEBUG] File content (hex): {:02x?}", content);
                Ok(Some(content))
            },
            Err(e) => {
                println!("[DEBUG] Failed to read file: {}", e);
                Ok(None)
            },
        }
    }
} 