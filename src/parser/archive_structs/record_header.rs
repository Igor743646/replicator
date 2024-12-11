use crate::common::types::{TypeRecordScn, TypeScn, TypeTimestamp};


#[derive(Debug, Default)]
pub struct RecordHeaderExpansion {
    pub record_num          : u16,
    pub record_num_max      : u16,
    pub records_count       : u32,
    pub records_scn         : TypeScn,
    pub scn1                : TypeScn,
    pub scn2                : TypeScn,
    pub records_timestamp   : TypeTimestamp,
}

impl std::fmt::Display for RecordHeaderExpansion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Num/NumMax: {}/{} Records count: {}\nRecord SCN: {}\nSCN1: {}\nSCN2: {}\nTimestamp: {}", 
                self.record_num, self.record_num_max, self.records_count, self.records_scn, self.scn1, self.scn2, self.records_timestamp)
    }
}

#[derive(Debug, Default)]
pub struct RecordHeader {
    pub record_size : u32,
    pub vld : u8,
    pub scn : TypeRecordScn,
    pub sub_scn : u16,
    pub container_uid : Option<u32>,
    pub expansion : Option<RecordHeaderExpansion>,
}

impl std::fmt::Display for RecordHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record size: {} VLD: {:02X}\nRecord SCN: {} Sub SCN: {}\n", self.record_size, self.vld, self.scn, self.sub_scn)?;
        if let Some(con_id) = self.container_uid {
            write!(f, "Container id: {}\n", con_id)?;
        }
        if let Some(ref ext) = self.expansion {
            write!(f, "{}\n", ext)?;
        }
        Ok(())
    }
}
