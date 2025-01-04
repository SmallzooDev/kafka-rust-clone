use std::path::PathBuf;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use bytes::{Buf, Bytes};
use async_trait::async_trait;
use crate::Result;
use crate::ports::outgoing::metadata_store::MetadataStore;
use crate::domain::message::{TopicMetadata, PartitionMetadata};
use crate::domain::error::DomainError;

pub struct KraftMetadataStore {
    log_dir: PathBuf,
}

impl KraftMetadataStore {
    pub fn new(log_dir: PathBuf) -> Self {
        Self { log_dir }
    }

    fn get_metadata_log_path(&self) -> PathBuf {
        self.log_dir.join("__cluster_metadata-0").join("00000000000000000000.log")
    }

    fn read_record(&self, file: &mut File) -> io::Result<Option<Record>> {
        let mut size_buf = [0u8; 4];
        if file.read_exact(&mut size_buf).is_err() {
            return Ok(None);  // 파일의 끝에 도달한 경우
        }
        
        let size = i32::from_be_bytes(size_buf);
        let mut record_buf = vec![0u8; size as usize];
        file.read_exact(&mut record_buf)?;
        
        Ok(Some(Record {
            data: Bytes::from(record_buf),
        }))
    }

    fn parse_topic_record(&self, record: Record) -> Option<TopicMetadata> {
        let mut buf = record.data;
        
        // 메타데이터에 필요한 최소 바이트(53)가 있는지 확인한다
        // (8: base offset, 4: record length, 4: partition leader epoch, 1: magic,
        //  4: crc, 2: attributes, 4: last offset delta, 8: first timestamp,
        //  8: max timestamp, 8: producer id, 2: producer epoch, 4: base sequence)
        if buf.remaining() < 53 {
            return None;
        }
        
        // 레코드 메타데이터를 건너뛴다
        buf.advance(8);  // base offset
        buf.advance(4);  // record length
        buf.advance(4);  // partition leader epoch
        buf.advance(1);  // magic
        buf.advance(4);  // crc
        buf.advance(2);  // attributes
        buf.advance(4);  // last offset delta
        buf.advance(8);  // first timestamp
        buf.advance(8);  // max timestamp
        buf.advance(8);  // producer id
        buf.advance(2);  // producer epoch
        buf.advance(4);  // base sequence
        
        // 토픽 이름 길이를 읽을 수 있는지 확인한다
        if buf.remaining() < 2 {
            return None;
        }
        
        // 토픽 이름을 읽는다
        let name_len = buf.get_i16() as usize;
        if buf.remaining() < name_len {
            return None;
        }
        let mut name_buf = vec![0u8; name_len];
        name_buf.copy_from_slice(&buf.copy_to_bytes(name_len));
        let name = String::from_utf8(name_buf).ok()?;
        
        // 토픽 ID를 읽을 수 있는지 확인한다
        if buf.remaining() < 16 {
            return None;
        }
        
        // 토픽 ID(UUID)를 읽는다
        let mut topic_id = [0u8; 16];
        topic_id.copy_from_slice(&buf.copy_to_bytes(16));
        
        // 파티션 개수를 읽을 수 있는지 확인한다
        if buf.remaining() < 4 {
            return None;
        }
        
        // 파티션 정보를 읽는다
        let partition_count = buf.get_i32() as usize;
        let mut partitions = Vec::with_capacity(partition_count);
        
        for _ in 0..partition_count {
            // 파티션 데이터를 읽을 수 있는지 확인한다 (인덱스 4바이트 + 리더 4바이트)
            if buf.remaining() < 8 {
                return None;
            }
            
            let partition_index = buf.get_i32();
            let leader_id = buf.get_i32();
            
            // 레플리카 노드 개수를 읽을 수 있는지 확인한다
            if buf.remaining() < 4 {
                return None;
            }
            
            // 레플리카 노드 목록을 읽는다
            let replica_count = buf.get_i32() as usize;
            if buf.remaining() < replica_count * 4 {
                return None;
            }
            let mut replica_nodes = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replica_nodes.push(buf.get_i32());
            }
            
            // ISR 노드 개수를 읽을 수 있는지 확인한다
            if buf.remaining() < 4 {
                return None;
            }
            
            // ISR(In-Sync Replica) 노드 목록을 읽는다
            let isr_count = buf.get_i32() as usize;
            if buf.remaining() < isr_count * 4 {
                return None;
            }
            let mut isr_nodes = Vec::with_capacity(isr_count);
            for _ in 0..isr_count {
                isr_nodes.push(buf.get_i32());
            }
            
            partitions.push(PartitionMetadata::new(
                partition_index,
                leader_id,
                replica_nodes,
                isr_nodes,
            ));
        }
        
        Some(TopicMetadata::new(name, topic_id, partitions))
    }
}

#[async_trait]
impl MetadataStore for KraftMetadataStore {
    async fn get_topic_metadata(&self, topic_name: &str) -> Result<Option<TopicMetadata>> {
        let log_path = self.get_metadata_log_path();
        
        // 메타데이터 로그 파일이 없는 경우 None을 반환한다
        let mut file = match File::open(log_path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };
        
        // 파일에서 레코드를 하나씩 읽어가며 토픽을 찾는다
        while let Some(record) = self.read_record(&mut file)
            .map_err(|_| DomainError::InvalidRequest)? 
        {
            if let Some(metadata) = self.parse_topic_record(record) {
                if metadata.name == topic_name {
                    return Ok(Some(metadata));
                }
            }
        }
        
        Ok(None)
    }
}

struct Record {
    data: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use tempfile::tempdir;

    fn create_test_log_file(dir: &PathBuf, records: Vec<Vec<u8>>) -> io::Result<()> {
        // 메타데이터 로그 디렉토리를 생성한다
        fs::create_dir_all(dir.join("__cluster_metadata-0"))?;
        let mut file = File::create(dir.join("__cluster_metadata-0/00000000000000000000.log"))?;
        
        // 각 레코드의 크기와 데이터를 기록한다
        for record in records {
            let size = record.len() as i32;
            file.write_all(&size.to_be_bytes())?;
            file.write_all(&record)?;
        }
        
        Ok(())
    }

    fn create_topic_record(name: &str, topic_id: [u8; 16], partitions: Vec<i32>) -> Vec<u8> {
        let mut record = Vec::new();
        
        // 레코드 메타데이터를 기록한다
        record.extend_from_slice(&[0u8; 8]);  // base offset
        record.extend_from_slice(&[0u8; 4]);  // record length
        record.extend_from_slice(&[0u8; 4]);  // partition leader epoch
        record.push(0);                       // magic
        record.extend_from_slice(&[0u8; 4]);  // crc
        record.extend_from_slice(&[0u8; 2]);  // attributes
        record.extend_from_slice(&[0u8; 4]);  // last offset delta
        record.extend_from_slice(&[0u8; 8]);  // first timestamp
        record.extend_from_slice(&[0u8; 8]);  // max timestamp
        record.extend_from_slice(&[0u8; 8]);  // producer id
        record.extend_from_slice(&[0u8; 2]);  // producer epoch
        record.extend_from_slice(&[0u8; 4]);  // base sequence
        
        // 토픽 이름을 기록한다
        record.extend_from_slice(&(name.len() as i16).to_be_bytes());
        record.extend_from_slice(name.as_bytes());
        
        // 토픽 ID를 기록한다
        record.extend_from_slice(&topic_id);
        
        // 파티션 정보를 기록한다
        record.extend_from_slice(&(partitions.len() as i32).to_be_bytes());  // 파티션 개수
        
        for partition_index in partitions {
            record.extend_from_slice(&partition_index.to_be_bytes());  // 파티션 인덱스
            record.extend_from_slice(&0i32.to_be_bytes());  // 리더 ID
            
            // 레플리카 노드 정보를 기록한다
            record.extend_from_slice(&1i32.to_be_bytes());  // 레플리카 개수
            record.extend_from_slice(&0i32.to_be_bytes());  // 노드 ID
            
            // ISR 노드 정보를 기록한다
            record.extend_from_slice(&1i32.to_be_bytes());  // ISR 개수
            record.extend_from_slice(&0i32.to_be_bytes());  // 노드 ID
        }
        
        record
    }

    #[tokio::test]
    async fn test_get_topic_metadata_existing_topic() -> Result<()> {
        let temp_dir = tempdir().unwrap();
        let topic_id = [1u8; 16];
        let records = vec![
            create_topic_record("test-topic", topic_id, vec![0]),
        ];
        create_test_log_file(&temp_dir.path().to_path_buf(), records).unwrap();
        
        let store = KraftMetadataStore::new(temp_dir.path().to_path_buf());
        let metadata = store.get_topic_metadata("test-topic").await?;
        
        assert!(metadata.is_some());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.name, "test-topic");
        assert_eq!(metadata.topic_id, topic_id);
        assert_eq!(metadata.partitions.len(), 1);
        assert_eq!(metadata.partitions[0].partition_index, 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_topic_metadata_nonexistent_topic() -> Result<()> {
        let temp_dir = tempdir().unwrap();
        let topic_id = [1u8; 16];
        let records = vec![
            create_topic_record("other-topic", topic_id, vec![0]),
        ];
        create_test_log_file(&temp_dir.path().to_path_buf(), records).unwrap();
        
        let store = KraftMetadataStore::new(temp_dir.path().to_path_buf());
        let metadata = store.get_topic_metadata("test-topic").await?;
        
        assert!(metadata.is_none());
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_topic_metadata_multiple_partitions() -> Result<()> {
        let temp_dir = tempdir().unwrap();
        let topic_id = [1u8; 16];
        let records = vec![
            create_topic_record("test-topic", topic_id, vec![0, 1]),
        ];
        create_test_log_file(&temp_dir.path().to_path_buf(), records).unwrap();
        
        let store = KraftMetadataStore::new(temp_dir.path().to_path_buf());
        let metadata = store.get_topic_metadata("test-topic").await?;
        
        assert!(metadata.is_some());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.partitions.len(), 2);  // Should have both partitions
        assert_eq!(metadata.partitions[0].partition_index, 0);
        assert_eq!(metadata.partitions[1].partition_index, 1);
        
        Ok(())
    }
} 