use std::fmt::{Formatter, Debug, Display};

#[derive(Copy, Clone, PartialEq)]
pub struct TypeRecordScn(u64);

impl Default for TypeRecordScn {
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

impl Debug for TypeRecordScn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for TypeRecordScn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
