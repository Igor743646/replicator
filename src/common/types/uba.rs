use std::fmt::{Formatter, Debug, Display};

#[derive(Debug, Default)]
pub struct TypeUba {
    pub data : u64,
}

impl TypeUba {
    pub fn new(data : u64) -> Self {
        Self {
            data
        }
    }
}

impl Display for TypeUba {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "block: {} record: {} sequence: {}", self.data & u32::MAX as u64, (self.data >> 48) & 0xFF, (self.data >> 32) & 0xFFFF)
    }
}
