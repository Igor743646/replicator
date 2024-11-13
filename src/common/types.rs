use std::{default, fmt::{Debug, Display}};


#[derive(Debug, PartialEq)]
pub struct TypeScn(u64);

impl default::Default for TypeScn {
    fn default() -> Self {
        Self {0 : 0xFFFFFFFFFFFFFFFF}
    }
}

impl From<u64> for TypeScn {
    fn from(val: u64) -> Self {
        Self {0 : val}
    }
}

impl std::fmt::Display for TypeScn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        std::unimplemented!();
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct TypeSeq(u32);

impl default::Default for TypeSeq {
    fn default() -> Self {
        Self {0 : 0}
    }
}

impl From<u32> for TypeSeq {
    fn from(val: u32) -> Self {
        Self {0 : val}
    }
}

impl From<u64> for TypeSeq {
    fn from(val: u64) -> Self {
        Self {0 : val as u32}
    }
}

impl Display for TypeSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type TypeConId = i16;

#[derive(Debug, Default)]
pub struct TypeRBA {
    block_number    : u32,
    sequence        : u32,
    offset          : u16,
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
