use std::fmt::{Formatter, Debug, Display};

#[derive(Debug, Default)]
pub struct TypeUba(u64);

impl TypeUba {
    pub fn new(data : u64) -> Self {
        Self(data)
    }

    pub fn block(&self) -> u32 {
        (self.0 & u32::MAX as u64) as u32
    }

    pub fn record(&self) -> u8 {
        ((self.0 >> 48) & 0xFF) as u8
    }

    pub fn sequence(&self) -> u16 {
        ((self.0 >> 32) & 0xFFFF) as u16
    }
}

impl Display for TypeUba {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.block(), self.record(), self.sequence())
    }
}
