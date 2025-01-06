use crate::adapters::incoming::protocol::messages::ErrorCode;
use crate::adapters::outgoing::protocol::kraft_record::{RecordBatch, RecordValue};
use crate::application::error::ApplicationError;
use crate::domain::message::TopicMetadata;
use crate::domain::message::{Partition};
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use std::path::PathBuf;
use tokio::fs::read;

pub struct KraftMetadataStore {
    log_dir: PathBuf,
}

impl KraftMetadataStore {
    pub fn new(log_dir: PathBuf) -> Self {
        Self { log_dir }
    }

    fn get_metadata_log_path(&self) -> PathBuf {
        let path = self.log_dir.join("__cluster_metadata-0").join("00000000000000000000.log");
        path
    }
}

#[async_trait]
impl MetadataStore for KraftMetadataStore {
    async fn get_topic_metadata(&self, requested_topics: Vec<String>) -> Result<Option<TopicMetadata>, ApplicationError> {
        let path = self.get_metadata_log_path();
        let content = read(&path).await.map_err(ApplicationError::Io)?;
        let mut data = BytesMut::with_capacity(content.len());
        data.extend_from_slice(&content);
        let mut data = data.freeze();

        let mut topics = Vec::new();
        let mut topic_id =  "00000000-0000-0000-0000-000000000000".to_string();
        let mut topic_error_code = ErrorCode::UnknownTopicOrPartition;
        let topic_authorized_operations = 0x0DF;

        println!("[DEBUG] Requested topics: {:?}", requested_topics);

        while data.remaining() > 0 {
            let record_batch = RecordBatch::from_bytes(&mut data)?;
            println!("[DEBUG] Processing record batch with {} records", record_batch.records.len());

            for topic_name in &requested_topics {
                topic_id =  "00000000-0000-0000-0000-000000000000".to_string();
                let mut partitions = Vec::new();

                // find topic id and partition info in the records
                for rec in &record_batch.records {
                    let record_type = &rec.value;
                    if let Some(id) = match record_type {
                        RecordValue::Topic(ref topic) if topic.topic_name == *topic_name => {
                            println!("[DEBUG] Found topic match: {} with ID: {}", topic.topic_name, topic.topic_id);
                            Some(topic.topic_id.clone())
                        }
                        _ => None,
                    } {
                        topic_id = id;
                        topic_error_code = ErrorCode::None;
                    };

                    match record_type {
                        RecordValue::Partition(p) if p.topic_id == topic_id => {
                            println!("[DEBUG] Found partition for topic {}: partition_id={}, leader_id={}", topic_name, p.partition_id, p.leader_id);
                            partitions.push(Partition::new(
                                i16::from(ErrorCode::None),
                                p.partition_id,
                                p.leader_id,
                                p.leader_epoch,
                                p.replicas.clone(),
                                p.in_sync_replicas.clone(),
                                p.adding_replicas.clone(),
                                Vec::new(),
                                p.removing_replicas.clone(),
                            ));
                        }
                        _ => {}
                    }
                }

                if !partitions.is_empty() {
                    let topic = TopicMetadata {
                        error_code: i16::from(topic_error_code),
                        name: topic_name.to_string(),
                        topic_id: topic_id.clone(),
                        is_internal: false,
                        partitions,
                        topic_authorized_operations,
                    };
                    println!("[DEBUG] Created topic metadata for {}: {:?}", topic_name, topic);
                    topics.push(topic);
                }
            }
        }

        for requested_topic in &requested_topics {
            let mut topic_found = false;
            for topic in &topics {
                if topic.name == *requested_topic {
                    topic_found = true;
                }
            }
            if !topic_found {
                let error_topic = TopicMetadata {
                    error_code: i16::from(ErrorCode::UnknownTopicOrPartition),
                    name: requested_topic.to_string(),
                    topic_id: topic_id.clone(),
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations,
                };
                println!("[DEBUG] Created error topic metadata for {}: {:?}", requested_topic, error_topic);
                topics.push(error_topic);
            }
        }

        if topics.is_empty() {
            println!("[DEBUG] No topics found");
            Ok(None)
        } else {
            println!("[DEBUG] Returning topic metadata: {:?}", topics[0]);
            Ok(Some(topics[0].clone()))
        }
    }
}
