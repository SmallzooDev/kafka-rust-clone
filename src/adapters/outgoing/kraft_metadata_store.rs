use crate::application::error::ApplicationError;
use crate::domain::message::TopicMetadata;
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use std::io::Read;
use std::path::PathBuf;
use tokio::fs::read;
use tokio::io::AsyncReadExt;

pub struct KraftMetadataStore {
    log_dir: PathBuf,
}

impl KraftMetadataStore {
    pub fn new(log_dir: PathBuf) -> Self {
        Self { log_dir }
    }

    fn get_metadata_log_path(&self) -> PathBuf {
        let path = self.log_dir.join("__cluster_metadata-0").join("00000000000000000000.log");
        println!("[METADATA] Looking for metadata file at: {:?}", path);
        path
    }
}

#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    pub batches: Vec<MetadataBatch>,
    pub topics: Vec<TopicMetadata>,
}

#[async_trait]
impl MetadataStore for KraftMetadataStore {
    async fn get_topic_metadata(&self, topic_name: &str) -> std::result::Result<Option<TopicMetadata>, ApplicationError> {
        let path = self.get_metadata_log_path();
        let content = read(&path).await.map_err(ApplicationError::Io)?;
        
        // TODO: 여기서 content를 파싱하여 ClusterMetadata를 생성

        Ok(None) // 임시 반환값
    }
}

#[derive(Debug, Clone)]
pub enum Record {
    FeatureLevel {
        frame_version: u8,
        version: u8,
        name: String,
        value: Vec<u8>,
    },
    Topic {
        frame_version: u8,
        version: u8,
        name: String,
        uuid: [u8; 16],
    },
    Partition {
        partition_id: u32,
        topic_uuid: [u8; 16],
        leader_id: u32,
        leader_epoch: u32,
        replica_nodes: Vec<u32>,
        in_sync_replica_nodes: Vec<u32>,
        partition_epoch: u32,
        directories: Vec<[u8; 16]>,
    }
}

#[derive(Debug, Clone)]
pub struct MetadataBatch {
    pub base_offset: u64,
    pub batch_length: u32,
    pub partition_leader_epoch: u32,
    pub magic_byte: u8,
    pub crc: u32,
    pub attributes: u16,
    pub last_offset_delta: u32,
    pub base_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub length: i64,  // varint
    pub attributes: u8,
    pub timestamp_delta: i64,  // varint
    pub offset_delta: i64,  // varint
    pub key_length: i64,  // varint
    pub key: Option<Vec<u8>>,
    pub value_length: i64,  // varint
}