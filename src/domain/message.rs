#[derive(Debug, Clone, PartialEq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,     
    pub topic_id: String, 
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Partition {
    pub error_code: i16,
    pub partition_index: u32,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub eligible_leader_replicas: Vec<u32>,
    pub last_known_eligible_leader_replicas: Vec<u32>,
    pub off_line_replicas: Vec<u32>,
}

impl Partition {
    pub fn new(
        error_code: i16,
        partition_index: u32,
        leader_id: u32,
        leader_epoch: u32,
        replicas: Vec<u32>,
        in_sync_replicas: Vec<u32>,
        eligible_leader_replicas: Vec<u32>,
        last_known_eligible_leader_replicas: Vec<u32>,
        off_line_replicas: Vec<u32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            in_sync_replicas,
            eligible_leader_replicas,
            last_known_eligible_leader_replicas,
            off_line_replicas,
        }
    }
} 