use std::default;


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
        write!(f, "{}", self.0);
        std::unimplemented!();
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct TypeConId(i16);

impl default::Default for TypeConId {
    fn default() -> Self {
        Self {0 : -1}
    }
}

impl From<i16> for TypeConId {
    fn from(val: i16) -> Self {
        Self {0 : val}
    }
}