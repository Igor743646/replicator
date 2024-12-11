use std::fmt::{Formatter, Debug, Display};

#[derive(Copy, Clone, PartialEq)]
pub struct TypeScn(u64);

impl Default for TypeScn {
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

impl Debug for TypeScn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:04X}.{:08X})", self.0, (self.0 >> 48) & 0xFFFF, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}

impl Display for TypeScn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:04X}.{:04X}.{:08X})", self.0, (self.0 >> 48) & 0xFFFF, (self.0 >> 32) & 0xFFFF, self.0 & 0xFFFFFFFF)
    }
}
