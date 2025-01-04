# Kafka Metadata File Analysis

## Raw Metadata File Content
```
[00, 00, 00, 00, 00, 00, 00, 01, 00, 00, 00, 4f, 00, 00, 00, 01, 02, b0, 69, 45, 7c, 00, 00, 00, 00, 00, 00, 00, 00, 01, 91, e0, 5a, f8, 18, 00, 00, 01, 91, e0, 5a, f8, 18, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, 00, 00, 00, 01, 3a, 00, 00, 00, 01, 2e, 01, 0c, 00, 11, 6d, 65, 74, 61, 64, 61, 74, 61, 2e, 76, 65, 72, 73, 69, 6f, 6e, 00, 14, 00, 00, 00, 00, 00, 00, 00, 00, 00, 02, 00, 00, 00, 9a, 00, 00, 00, 01, 02, 82, 6a, 11, eb, 00, 00, 00, 00, 00, 01, 00, 00, 01, 91, e0, 5b, 2d, 15, 00, 00, 01, 91, e0, 5b, 2d, 15, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, 00, 00, 00, 02, 3c, 00, 00, 00, 01, 30, 01, 02, 00, 04, 62, 61, 72, 00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 38, 00, 00, 90, 01, 00, 00, 02, 01, 82, 01, 01, 03, 01, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 38, 02, 00, 00, 00, 01, 02, 00, 00, 00, 01, 01, 01, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 00, 02, 10, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 00, 00, 04, 00, 00, 00, 9a, 00, 00, 00, 01, 02, 2a, 9d, 48, 3e, 00, 00, 00, 00, 00, 01, 00, 00, 01, 91, e0, 5b, 2d, 15, 00, 00, 01, 91, e0, 5b, 2d, 15, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, 00, 00, 00, 02, 3c, 00, 00, 00, 01, 30, 01, 02, 00, 04, 62, 61, 7a, 00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 66, 00, 00, 90, 01, 00, 00, 02, 01, 82, 01, 01, 03, 01, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 66, 02, 00, 00, 00, 01, 02, 00, 00, 00, 01, 01, 01, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 00, 02, 10, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 01, 00, 00, 90, 01, 00, 00, 04, 01, 82, 01, 01, 03, 01, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 96, 02, 00, 00, 00, 01, 02, 00, 00, 00, 01, 01, 01, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 00, 02, 10, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 01, 00, 00]
```

## File Structure Analysis

### Batch 1: Feature Level Record
```
Batch Header:
[00, 00, 00, 00, 00, 00, 00, 01] - base_offset = 1
[00, 00, 00, 4f] - batch_length = 79 (0x4f)
[00, 00, 00, 01] - partition_leader_epoch = 1
[02] - magic_byte = 2
[b0, 69, 45, 7c] - crc
[00, 00] - attributes = 0
[00, 00, 00, 00] - last_offset_delta = 0
[00, 00, 01, 91, e0, 5a, f8, 18] - base_timestamp
[00, 00, 01, 91, e0, 5a, f8, 18] - max_timestamp
[ff, ff, ff, ff, ff, ff, ff, ff] - producer_id = -1
[ff, ff] - producer_epoch = -1
[ff, ff, ff, ff] - base_sequence = -1
[00, 00, 00, 01] - records_length = 1

Record:
[3a] - length varint = 29
[00] - attributes = 0
[00] - timestamp_delta varint = 0
[01] - offset_delta varint = -1
[2e] - value_length varint = 23
Value:
[01] - frame_version = 1
[0c] - type = 12 (feature level record)
[00] - version = 0
[11] - string length = 17
[6d, 65, 74, 61, 64, 61, 74, 61, 2e, 76, 65, 72, 73, 69, 6f, 6e] - "metadata.version"
[00, 14, 00] - additional data
```

### Batch 2: "bar" Topic
```
Batch Header:
[00, 00, 00, 00, 00, 00, 00, 02] - base_offset = 2
[00, 00, 00, 9a] - batch_length = 154 (0x9a)
[00, 00, 00, 01] - partition_leader_epoch = 1
[02] - magic_byte = 2
[82, 6a, 11, eb] - crc
[00, 00] - attributes = 0
[00, 00, 00, 00] - last_offset_delta = 0
[00, 00, 01, 91, e0, 5b, 2d, 15] - base_timestamp
[00, 00, 01, 91, e0, 5b, 2d, 15] - max_timestamp
[ff, ff, ff, ff, ff, ff, ff, ff] - producer_id = -1
[ff, ff] - producer_epoch = -1
[ff, ff, ff, ff] - base_sequence = -1
[00, 00, 00, 02] - records_length = 2

Record 1 (Topic):
[3c] - length varint = 30
[01] - attributes = 1
[30] - timestamp_delta = 24
[01] - frame_version = 1
[02] - type = 2 (topic record)
[00] - version = 0
[04] - string length = 4
[62, 61, 72] - "bar"
[00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 38] - topic UUID

Record 2 (Partition):
[90, 01] - length varint = 144
[00] - attributes = 0
[02] - type = 3 (partition record)
[01] - partition_id = 1
[82, 01] - replica_nodes array length = 1
[01] - replica_node = 1
[03] - in_sync_replica_nodes array length = 1
[01] - in_sync_replica_node = 1
[00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 38] - topic UUID
[02, 00, 00, 00, 01] - leader_id = 1
[01, 01] - leader_epoch = 1
[00, 00, 00, 01] - partition_epoch = 1
[02, 10] - directories array length = 1
[00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 01] - directory UUID
```

### Batch 3: "baz" Topic
```
Batch Header:
[00, 00, 00, 00, 00, 00, 00, 02] - base_offset = 2
[00, 00, 00, 9a] - batch_length = 154
[00, 00, 00, 01] - partition_leader_epoch = 1
[02] - magic_byte = 2
[2a, 9d, 48, 3e] - crc
... (헤더 나머지 동일)
[00, 00, 00, 02] - records_length = 2

Record 1 (Topic):
[3c] - length varint = 30
[01] - attributes = 1
[30] - timestamp_delta = 24
[01] - frame_version = 1
[02] - type = 2 (topic record)
[00] - version = 0
[04] - string length = 4
[62, 61, 7a] - "baz"
[00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 66] - topic UUID

Record 2 (Partition):
... (구조는 "bar" 토픽의 파티션과 동일)
```

### Batch 4: "pax" Topic
```
Batch Header:
[00, 00, 00, 00, 00, 00, 00, 02] - base_offset = 2
[00, 00, 00, e4] - batch_length = 228
[00, 00, 00, 01] - partition_leader_epoch = 1
[02] - magic_byte = 2
[4e, d4, 85, 58] - crc
... (헤더 나머지 동일)
[00, 00, 00, 03] - records_length = 3

Record 1 (Topic):
[3c] - length varint = 30
[01] - attributes = 1
[30] - timestamp_delta = 24
[01] - frame_version = 1
[02] - type = 2 (topic record)
[00] - version = 0
[04] - string length = 4
[70, 61, 78] - "pax"
[00, 00, 00, 00, 00, 00, 40, 00, 80, 00, 00, 00, 00, 00, 00, 96] - topic UUID

Records 2 & 3 (Partitions):
... (구조는 이전 토픽들의 파티션과 동일)
```

## Object Structure
```rust
// 메타데이터 파일의 각 배치
struct MetadataBatch {
    base_offset: u64,
    batch_length: u32,
    partition_leader_epoch: u32,
    magic_byte: u8,
    crc: u32,
    attributes: u16,
    last_offset_delta: u32,
    base_timestamp: u64,
    max_timestamp: u64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}

// 각 레코드의 헤더
struct RecordHeader {
    length: i64,  // varint
    attributes: u8,
    timestamp_delta: i64,  // varint
    offset_delta: i64,  // varint
    key_length: i64,  // varint
    key: Option<Vec<u8>>,
    value_length: i64,  // varint
}

// 레코드 타입
enum Record {
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
```

## 파싱 순서
1. 파일의 끝까지 반복:
   - 배치 헤더 읽기 (base_offset부터 records_length까지)
   - records_length만큼 레코드 읽기
   - 각 레코드마다:
     1. 레코드 헤더 읽기 (length부터 value_length까지)
     2. value_length만큼 데이터 읽기
     3. 데이터의 type 필드에 따라 적절한 Record 타입으로 파싱

## 주의사항
1. varint 읽을 때 MSB가 1이면 계속 읽기
2. 문자열 길이는 varint로 인코딩됨
3. 배열 길이도 varint로 인코딩됨
4. UUID는 항상 16바이트
5. 각 배치의 끝까지 정확히 읽어야 다음 배치로 넘어갈 수 있음