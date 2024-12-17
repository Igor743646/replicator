use std::fmt::{Formatter, Debug, Display};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeXid {
    pub undo_segment_number : u16,
    pub slot_number : u16,
    pub sequence_number : u32,
}

impl TypeXid {
    pub fn new(usn : u16, slt : u16, seq : u32) -> Self {
        Self {
            undo_segment_number : usn,
            slot_number : slt, 
            sequence_number : seq,
        }
    }
}

impl From<u64> for TypeXid {
    fn from(value: u64) -> Self {
        Self {
            undo_segment_number : ((value >> 48) & 0xFFFF) as u16,
            slot_number : ((value >> 32) & 0xFFFF) as u16,
            sequence_number : (value & 0xFFFFFFFF) as u32,
        }
    }
}

impl Display for TypeXid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.undo_segment_number, self.slot_number, self.sequence_number)
    }
}
