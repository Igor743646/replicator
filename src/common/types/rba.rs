use std::fmt::{Formatter, Debug, Display};

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.sequence, self.block_number, self.offset)
    }
}
