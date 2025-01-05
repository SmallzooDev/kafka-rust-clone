use crate::application::error::ApplicationError;
use crate::domain::message::TopicMetadata;
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use std::io::Read;
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
        println!("[METADATA] Looking for metadata file at: {:?}", path);
        path
    }
}

#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    pub batches: Vec<MetadataBatch>,
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

#[async_trait]
impl MetadataStore for KraftMetadataStore {
    async fn get_topic_metadata(&self, topic_name: &str) -> std::result::Result<Option<TopicMetadata>, ApplicationError> {
        let path = self.get_metadata_log_path();
        let content = read(&path).await.map_err(ApplicationError::Io)?;
        
        println!("[METADATA] Raw bytes: {:02X?}", content);
        
        let content_slice = content.as_slice();
        let mut cursor = std::io::Cursor::new(content_slice);
        let mut found_topic_uuid = None;
        let mut partitions = Vec::new();

        // Keep reading batches until we reach the end of the file or find an error
        while cursor.position() < content_slice.len() as u64 {
            let batch_start_position = cursor.position();
            println!("[METADATA] Reading batch at position: {}", batch_start_position);
            
            let base_offset = read_u64(&mut cursor)?;
            let batch_length = read_u32(&mut cursor)?;
            println!("[METADATA] Batch length: {}", batch_length);
            
            let mut batch = MetadataBatch {
                base_offset,
                batch_length,
                partition_leader_epoch: read_u32(&mut cursor)?,
                magic_byte: read_u8(&mut cursor)?,
                crc: read_u32(&mut cursor)?,
                attributes: read_u16(&mut cursor)?,
                last_offset_delta: read_u32(&mut cursor)?,
                base_timestamp: read_u64(&mut cursor)?,
                max_timestamp: read_u64(&mut cursor)?,
                producer_id: read_i64(&mut cursor)?,
                producer_epoch: read_i16(&mut cursor)?,
                base_sequence: read_i32(&mut cursor)?,
                records: Vec::new(),
            };

            let records_length = read_u32(&mut cursor)?;
            println!("[METADATA] Records length in current batch: {}", records_length);

            for record_index in 0..records_length {
                println!("[METADATA] Parsing record {}/{}", record_index + 1, records_length);
                
                let record_start_position = cursor.position();
                println!("[METADATA] Record start position: {}", record_start_position);
                let mut next_byte = read_u8(&mut cursor)?;
                println!("[METADATA] First byte of record: {:02X}", next_byte);
                cursor.set_position(record_start_position);  // Reset position
                
                let record_length = read_varint(&mut cursor)?;
                println!("[METADATA] Record length (VARINT): {}", record_length);
                
                let attributes = read_u8(&mut cursor)?;
                println!("[METADATA] Record attributes: {:02X}", attributes);
                
                let timestamp_delta = read_varint(&mut cursor)?;
                println!("[METADATA] Timestamp delta: {}", timestamp_delta);
                
                let offset_delta = read_varint(&mut cursor)?;
                println!("[METADATA] Offset delta: {}", offset_delta);
                
                let key_length = read_varint(&mut cursor)?;
                println!("[METADATA] Key length: {}", key_length);
                
                let value_length = read_varint(&mut cursor)?;
                println!("[METADATA] Value length: {}", value_length);

                if value_length > 0 {
                    let frame_version = read_u8(&mut cursor)?;
                    let record_type = read_u8(&mut cursor)?;
                    let version = read_u8(&mut cursor)?;
                    println!("[METADATA] Record header - frame_version: {}, type: {:02X}, version: {}", frame_version, record_type, version);
                    
                    match record_type {
                        0x0c => { // FeatureLevel
                            let name_length = read_varint(&mut cursor)? as usize;
                            let mut name_bytes = vec![0u8; name_length];
                            cursor.read_exact(&mut name_bytes).map_err(|e| ApplicationError::Io(e))?;
                            let name = String::from_utf8(name_bytes[..name_length-1].to_vec())
                                .map_err(|_| ApplicationError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8")))?;
                            
                            let feature_level = read_u16(&mut cursor)?;
                            let _tagged_fields_count = read_varint(&mut cursor)?;
                            
                            batch.records.push(Record::FeatureLevel {
                                frame_version,
                                version,
                                name,
                                value: feature_level.to_be_bytes().to_vec(),
                            });
                        }
                        0x02 => { // Topic
                            let name_length = read_varint(&mut cursor)? as usize;
                            println!("[METADATA] Topic record - name_length (VARINT): {}", name_length);
                            
                            let mut name_bytes = vec![0u8; name_length];
                            cursor.read_exact(&mut name_bytes).map_err(|e| ApplicationError::Io(e))?;
                            println!("[METADATA] Topic record - name_bytes: {:02X?}", name_bytes);
                            
                            let name = String::from_utf8(name_bytes[..name_length-1].to_vec())
                                .map_err(|_| ApplicationError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8")))?;
                            println!("[METADATA] Topic record - name: {}", name);
                            
                            println!("[METADATA] Current buffer position before UUID: {}", cursor.position());
                            
                            let mut uuid = [0u8; 16];
                            cursor.read_exact(&mut uuid)?;
                            
                            // FIXME: Topic Record의 UUID가 한 바이트씩 앞으로 밀려서 읽히는 현상 보정
                            // 정확한 원인은 불명확하나 Topic Record에서만 발생하는 문제
                            for i in (1..16).rev() {
                                uuid[i] = uuid[i-1];
                            }
                            uuid[0] = 0;
                            
                            println!("[METADATA] Topic record - uuid bytes: {:02X?}", uuid);
                            println!("[METADATA] Current buffer position after UUID: {}", cursor.position());
                            
                            let _tagged_fields_count = read_varint(&mut cursor)?;
                            println!("[METADATA] Topic record - tagged_fields_count: {}", _tagged_fields_count);
                            println!("[METADATA] Current buffer position after tagged_fields_count: {}", cursor.position());
                            
                            let topic_name_clone = name.clone();
                            batch.records.push(Record::Topic {
                                frame_version,
                                version,
                                name: topic_name_clone,
                                uuid,
                            });
                            
                            // If this is the topic we're looking for, store its UUID
                            if name == topic_name {
                                println!("[METADATA] Found matching topic: {} with UUID: {:02X?}", name, uuid);
                                found_topic_uuid = Some(uuid);
                            } else {
                                println!("[METADATA] Topic name mismatch - looking for: {}, found: {}", topic_name, name);
                            }
                        }
                        0x03 => { // Partition
                            let partition_id = read_u32(&mut cursor)?;
                            
                            let mut topic_uuid = [0u8; 16];
                            cursor.read_exact(&mut topic_uuid).map_err(|e| ApplicationError::Io(e))?;
                            
                            let leader_id = read_u32(&mut cursor)?;
                            let leader_epoch = read_u32(&mut cursor)?;
                            
                            // Read replica nodes
                            let replica_count = read_varint(&mut cursor)? as usize;
                            let mut replica_nodes = Vec::with_capacity(replica_count);
                            for _ in 0..replica_count {
                                replica_nodes.push(read_u32(&mut cursor)?);
                            }
                            
                            // Read ISR nodes
                            let isr_count = read_varint(&mut cursor)? as usize;
                            let mut isr_nodes = Vec::with_capacity(isr_count);
                            for _ in 0..isr_count {
                                isr_nodes.push(read_u32(&mut cursor)?);
                            }
                            
                            let partition_epoch = read_u32(&mut cursor)?;
                            
                            // Read directories
                            let dir_count = read_varint(&mut cursor)? as usize;
                            let mut directories = Vec::with_capacity(dir_count);
                            for _ in 0..dir_count {
                                let mut dir_uuid = [0u8; 16];
                                cursor.read_exact(&mut dir_uuid).map_err(|e| ApplicationError::Io(e))?;
                                directories.push(dir_uuid);
                            }
                            
                            let _tagged_fields_count = read_varint(&mut cursor)?;
                            
                            let partition = Record::Partition {
                                partition_id,
                                topic_uuid,
                                leader_id,
                                leader_epoch,
                                replica_nodes: replica_nodes.clone(),
                                in_sync_replica_nodes: isr_nodes.clone(),
                                partition_epoch,
                                directories,
                            };
                            
                            batch.records.push(partition);
                            
                            // If this partition belongs to our topic, store its metadata
                            if let Some(target_uuid) = found_topic_uuid {
                                println!("[METADATA] Comparing UUIDs - Target: {:02X?}, Found: {:02X?}", target_uuid, topic_uuid);
                                if topic_uuid == target_uuid {
                                    println!("[METADATA] Found matching partition {} for topic", partition_id);
                                    partitions.push(crate::domain::message::PartitionMetadata::new(
                                        partition_id as i32,
                                        leader_id as i32,
                                        replica_nodes.iter().map(|&x| x as i32).collect(),
                                        isr_nodes.iter().map(|&x| x as i32).collect(),
                                    ));
                                }
                            }
                        }
                        // Add other record types as needed
                        _ => return Err(ApplicationError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown record type"))),
                    }
                }
                
                // Skip to the end of the record based on record_length
                let record_end_position = record_start_position + record_length as u64;
                cursor.set_position(record_end_position);
                println!("[METADATA] Skipping to next record at position: {}", record_end_position);
            }
            
            // Skip to the next batch based on batch_length
            let batch_end_position = batch_start_position + batch_length as u64 + 12;  // +12 for base_offset(8) and batch_length(4)
            cursor.set_position(batch_end_position);
            println!("[METADATA] Skipping to next batch at position: {}", batch_end_position);
        }

        // Return topic metadata if we found both the topic and its partitions
        if let Some(uuid) = found_topic_uuid {
            Ok(Some(TopicMetadata::new(
                topic_name.to_string(),
                uuid,
                partitions,
            )))
        } else {
            Ok(None)
        }
    }
}

// Helper functions for reading binary data
fn read_u8(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u8, ApplicationError> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(buf[0])
}

fn read_u16(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u16, ApplicationError> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(u16::from_be_bytes(buf))
}

fn read_u32(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u32, ApplicationError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u64, ApplicationError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_i16(cursor: &mut std::io::Cursor<&[u8]>) -> Result<i16, ApplicationError> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_i32(cursor: &mut std::io::Cursor<&[u8]>) -> Result<i32, ApplicationError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_i64(cursor: &mut std::io::Cursor<&[u8]>) -> Result<i64, ApplicationError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf).map_err(ApplicationError::Io)?;
    Ok(i64::from_be_bytes(buf))
}

fn read_varint(cursor: &mut std::io::Cursor<&[u8]>) -> Result<i64, ApplicationError> {
    let mut value: i64 = 0;
    let mut shift = 0;
    
    loop {
        let byte = read_u8(cursor)?;
        println!("[METADATA] Reading varint byte: {:02X}", byte);
        value |= ((byte & 0x7f) as i64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(ApplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VarInt is too long",
            )));
        }
    }
    
    println!("[METADATA] Decoded varint value: {}", value);
    Ok(value)
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