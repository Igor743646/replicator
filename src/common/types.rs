use std::{default, fmt::{Debug, Display}};

#[derive(Copy, Clone, PartialEq)]
pub struct TypeScn(u64);

impl default::Default for TypeScn {
    fn default() -> Self {
        Self {0 : 0xFFFFFFFFFFFFFFFF}
    }
}

impl Into<u64> for TypeScn {
    fn into(self) -> u64 {
        self.0
    }
}

impl From<u64> for TypeScn {
    fn from(val: u64) -> Self {
        Self {0 : val}
    }
}

impl std::fmt::Debug for TypeScn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:04X}.{:08X})", self.0, (self.0 >> 48) & 0xFFFF, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}

impl std::fmt::Display for TypeScn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:04X}.{:08X})", self.0, (self.0 >> 48) & 0xFFFF, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}

#[derive(Copy, Clone, PartialEq)]
pub struct TypeRecordScn(u64);

impl default::Default for TypeRecordScn {
    fn default() -> Self {
        Self {0 : 0xFFFFFFFFFFFFFFFF}
    }
}

impl Into<u64> for TypeRecordScn {
    fn into(self) -> u64 {
        self.0
    }
}

impl From<u64> for TypeRecordScn {
    fn from(val: u64) -> Self {
        Self {0 : val}
    }
}

impl std::fmt::Debug for TypeRecordScn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:08X})", self.0, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}

impl std::fmt::Display for TypeRecordScn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:08X})", self.0, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}

pub type TypeSeq = u32;
pub type TypeConId = i16;

#[derive(Debug, Default)]
pub struct TypeRBA {
    pub block_number    : u32,
    pub sequence        : u32,
    pub offset          : u16,
}

impl TypeRBA {
    pub fn new(block_number : u32, sequence : u32, offset : u16) -> Self { 
        Self { block_number, sequence , offset }
    }
}

impl Display for TypeRBA {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{} (seq.bn.off)", self.sequence, self.block_number, self.offset)
    }
}

#[derive(Default)]
pub struct TypeTimestamp(u32);

impl TypeTimestamp {
    pub fn new(time : u32) -> Self {
        Self(time)
    }
}

impl From<u32> for TypeTimestamp {
    fn from(val: u32) -> Self {
        Self {0 : val}
    }
}

impl Debug for TypeTimestamp where TypeTimestamp : Display {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let temp: &dyn Display = self;
        temp.fmt(f)
    }
}

impl Display for TypeTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        let mut res = self.0;
        let ss = res % 60;
        res /= 60;
        let mi = res % 60;
        res /= 60;
        let hh = res % 24;
        res /= 24;
        let dd = (res % 31) + 1;
        res /= 31;
        let mm = (res % 12) + 1;
        res /= 12;
        let yy = res + 1988;
        
        write!(f, "{:04}-{:02}-{:02} {:02}:{:02}:{:02} ({})", yy, mm, dd, hh, mi, ss, self.0)
    }
}

#[derive(Debug, Default)]
pub struct TypeXid {
    pub undo_segment_number : u16,
    pub slot_number : u16,
    pub sequence_number : u32,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{} ({:04X}.{:04X}.{:08X})", self.undo_segment_number, self.slot_number, self.sequence_number, self.undo_segment_number, self.slot_number, self.sequence_number)
    }
}