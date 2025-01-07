use async_trait::async_trait;
use crate::Result;
use crate::domain::message::TopicMetadata;

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn get_topic_metadata(&self, topic_names: Vec<String>) -> Result<Option<Vec<TopicMetadata>>>;
} 