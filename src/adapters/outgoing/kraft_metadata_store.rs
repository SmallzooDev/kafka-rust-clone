use crate::application::error::ApplicationError;
use crate::domain::message::TopicMetadata;
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use std::io::Read;
use std::path::PathBuf;
use bytes::{Buf, BytesMut, Bytes, BufMut};
use tokio::fs::read;
use crate::domain::message::{ErrorCode, Partition};
use crate::adapters::outgoing::kraft_metadata_store::RecordValue::Partition as RecordPartition;

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

        while data.remaining() > 0 {
            let record_batch = RecordBatch::from_bytes(&mut data);

            for topic_name in &requested_topics {
                topic_id =  "00000000-0000-0000-0000-000000000000".to_string();
                let mut partitions = Vec::new();

                // find topic id and partition info in the records
                for rec in &record_batch.records {
                    let record_type = &rec.value;
                    if let Some(id) = match record_type {
                        RecordValue::Topic(ref topic) if topic.topic_name == *topic_name => {
                            Some(topic.topic_id.clone())
                        }
                        _ => None,
                    } {
                        topic_id = id;
                        topic_error_code = ErrorCode::None;
                    };

                    match record_type {
                        RecordPartition(p) if p.topic_id == topic_id => {
                            partitions.push(Partition::new(
                                ErrorCode::None,
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
                        error_code: topic_error_code,
                        name: topic_name.to_string(),
                        topic_id: topic_id.clone(),
                        is_internal: false,
                        partitions,
                        topic_authorized_operations,
                    };
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
                    error_code: ErrorCode::UnknownTopicOrPartition,
                    name: requested_topic.to_string(),
                    topic_id: topic_id.clone(),
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations,
                };
                topics.push(error_topic);
            }
        }

        if topics.is_empty() {
            Ok(None)
        } else {
            Ok(Some(topics[0].clone()))
        }
    }
}


#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Record {
    length: i64,
    /// Attributes is a 1-byte big-endian integer indicating the attributes of the record. Currently, this field is unused in the protocol.
    attributes: i8,
    /// Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
    timestamp_delta: i64,
    /// Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
    offset_delta: i64,
    /// Key is a byte array indicating the key of the record.
    key: Vec<u8>,
    /// Value Length is a signed variable size integer indicating the length of the value of the record.
    value_length: i64,
    /// Value is a byte array indicating the value of the record.
    pub value: RecordValue,
    headers: Vec<Header>,
}

impl Record {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let length = VarInt::deserialize(src);
        let attributes = src.get_i8();
        let timestamp_delta = VarInt::deserialize(src);
        let offset_delta = VarInt::deserialize(src);
        let key = CompactNullableBytes::deserialize(src);
        let value_length = VarInt::deserialize(src);
        let value = RecordValue::from_bytes(src);
        let headers = CompactArray::deserialize::<Header, Record>(src);

        Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        }
    }
}


impl RecordValue {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        // Frame Version is indicating the version of the format of the record.
        let frame_version = src.get_u8();
        assert_eq!(frame_version, 1);

        let record_type = src.get_u8();
        match record_type {
            2 => {
                // Topic Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let topic_name = CompactString::deserialize(src);
                let topic_id = Uuid::deserialize(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::Topic(TopicValue {
                    topic_name,
                    topic_id,
                })
            }
            3 => {
                // Partition Record Value
                let version = src.get_u8();
                assert_eq!(version, 1);
                let partition_id = src.get_u32();
                let topic_id = Uuid::deserialize(src);

                let replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let in_sync_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let removing_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let adding_replicas = CompactArray::deserialize::<_, PartitionValue>(src);

                let leader_id = src.get_u32();
                let leader_epoch = src.get_u32();
                let partition_epoch = src.get_u32();

                let directories = CompactArray::deserialize::<String, PartitionValue>(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);

                RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                })
            }

            12 => {
                // Feature Level Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let name = CompactString::deserialize(src);
                let level = src.get_u16();
                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::FeatureLevel(FeatureLevelValue { name, level })
            }

            _ => unimplemented!(),
        }
    }
}

pub struct CompactString;
impl CompactString {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::new();
        let len = s.len() as u8 + 1;
        b.put_u8(len);
        b.put(s.as_bytes());
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> String {
        let len = VarInt::deserialize(src); // string length + 1
        let string_len = if len > 1 { len as usize - 1 } else { 0 };
        let bytes = src.slice(..string_len);
        src.advance(string_len);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

pub struct VarInt;

impl VarInt {
    pub(crate) fn deserialize<T>(buf: &mut T) -> i64
    where
        T: bytes::Buf,
    {
        const MAX_BYTES: usize = 10;
        if buf.remaining() == 0 {
            panic!("buffer is empty")
        }

        let buf_len = buf.remaining();

        let mut b0 = buf.get_i8() as i64;
        let mut res = b0 & 0b0111_1111; // drop the MSB (continuation bit)
        let mut n_bytes = 1;

        while b0 & 0b1000_0000 != 0 && n_bytes <= MAX_BYTES {
            // highest bit (continuation bit) in the first byte is one, get another byte

            if buf.remaining() == 0 {
                if buf_len >= MAX_BYTES {
                    panic!("invalid varint")
                }
                panic!("buffer is too short ({} bytes) or invalid varint", buf_len)
            }

            let b1 = buf.get_i8() as i64;
            if buf.remaining() == 0 && b1 & 0b1000_0000 != 0 {
                // last byte still starts with 1

                if buf_len >= 8 {
                    panic!("invalid varint")
                }
                panic!("buffer is too short ({} bytes) or invalid varint", buf_len)
            }

            // drop the continuation bit and convert to big-endian
            res += (b1 & 0b0111_1111) << 7;

            n_bytes += 1;

            b0 = b1;
        }

        res
    }
}

impl Deserialize<String> for CompactString {
    fn deserialize(src: &mut Bytes) -> String {
        Self::deserialize(src)
    }
}

#[derive(Debug, Clone, Copy)]
struct Header;

impl Deserialize<Header> for Record {
    fn deserialize(_src: &mut Bytes) -> Header {
        Header
    }
}



#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FeatureLevelValue {
    name: String,
    level: u16,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PartitionValue {
    pub partition_id: u32,
    pub topic_id: String,
    pub replicas: Vec<u32>,
    /// The in-sync replicas of this partition
    pub in_sync_replicas: Vec<u32>,
    /// The replicas that we are in the process of removing
    pub removing_replicas: Vec<u32>,
    pub adding_replicas: Vec<u32>,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub partition_epoch: u32,
    pub directories: Vec<String>,
}
pub struct Uuid;

impl Uuid {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::with_capacity(32);
        b.extend_from_slice(&hex::decode(s.replace('-', "")).expect("valid UUID string"));
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> String {
        // 00000000-0000-0000-0000-000000000000
        let mut s = hex::encode(src.slice(..16));
        src.advance(16);
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        s
    }
}

impl Deserialize<u32> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> u32 {
        src.get_u32()
    }
}

impl Deserialize<String> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> String {
        Uuid::deserialize(src)
    }
}

#[derive(Debug, Clone)]
pub struct TopicValue {
    pub topic_name: String,
    pub topic_id: String, // UUID
}


#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,

    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let base_offset = src.get_i64();
        let batch_length = src.get_i32();
        let partition_leader_epoch = src.get_i32();
        let magic = src.get_i8();
        let crc = src.get_u32();
        let attributes = src.get_i16();
        let last_offset_delta = src.get_i32();
        let base_timestamp = src.get_i64();
        let max_timestamp = src.get_i64();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let records = NullableBytes::deserialize::<Record, RecordBatch>(src);

        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }
}
pub struct CompactArray;
impl CompactArray {
    pub fn serialize<T: Serialize>(items: &mut [T]) -> Bytes {
        let mut b = BytesMut::new();
        // COMPACT ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        let len = items.len() as u8 + 1;
        b.put_u8(len);

        for item in items.iter_mut() {
            b.put(item.serialize());
        }

        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = VarInt::deserialize(src); // array length + 1
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
        }

        items
    }
}



/// Represents a raw sequence of bytes or null.
/// For non-null values, first the length N is given as an INT32. Then N bytes follow.
/// A null value is encoded with length of -1 and there are no following bytes.
pub struct NullableBytes;

#[allow(dead_code)]
impl NullableBytes {
    pub fn serialize(bytes: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        let len = bytes.len() as i32 + 1;
        b.put_i32(len);
        b.put(bytes);
        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = src.get_i32();
        let items_len = if len == -1 { 0 } else { len as usize };

        let mut items = Vec::with_capacity(items_len);
        for i in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
        }
        items
    }
}


pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> T;
}

pub trait Serialize {
    fn serialize(&mut self) -> Bytes;
}

impl Deserialize<Record> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Record {
        Record::from_bytes(src)
    }
}

#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    pub batches: Vec<RecordBatch>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone)]
struct RecordHeader {
    length: i64,  // varint
    attributes: u8,
    timestamp_delta: i64,  // varint
    offset_delta: i64,  // varint
    value_length: i64,  // varint
}

pub struct CompactNullableBytes;

#[allow(dead_code)]
impl CompactNullableBytes {
    pub fn serialize(bytes: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        let len = bytes.len() as u8 + 1; // should be varint
        b.put_u8(len);
        b.put(bytes);
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> Vec<u8> {
        let len = VarInt::deserialize(src);
        let bytes_len = if len > 1 { len as usize - 1 } else { 0 };
        let bytes = src.slice(..bytes_len);
        src.advance(bytes_len);
        Vec::from(bytes)
    }
}
