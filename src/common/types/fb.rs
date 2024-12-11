use core::str;


#[derive(Default, Clone, Copy)]
pub struct TypeFb(u8);

impl TypeFb {
    pub fn is_next(&self) -> bool { (self.0 & 0b00000001) != 0 }
    pub fn is_prev(&self) -> bool { (self.0 & 0b00000010) != 0 }
    pub fn is_last(&self) -> bool { (self.0 & 0b00000100) != 0 }
    pub fn is_first(&self) -> bool { (self.0 & 0b00001000) != 0 }
    pub fn is_deleted(&self) -> bool { (self.0 & 0b00010000) != 0 }
    pub fn is_head(&self) -> bool { (self.0 & 0b00100000) != 0 }
    pub fn is_clustered(&self) -> bool { (self.0 & 0b01000000) != 0 }
    pub fn is_cluster_key(&self) -> bool { (self.0 & 0b10000000) != 0 }
}

impl std::fmt::Debug for TypeFb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl std::fmt::Display for TypeFb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = *b"--------";
        if self.is_next()        { s[7] = b'N'; }    // The last column continues in the Next piece
        if self.is_prev()        { s[6] = b'P'; }    // The first column continues from the Previous piece
        if self.is_last()        { s[5] = b'L'; }    // Last ctx piece
        if self.is_first()       { s[4] = b'F'; }    // First ctx piece
        if self.is_deleted()     { s[3] = b'D'; }    // Deleted row
        if self.is_head()        { s[2] = b'H'; }    // Head piece of row
        if self.is_clustered()   { s[1] = b'C'; }    // Clustered table member
        if self.is_cluster_key() { s[0] = b'K'; }    // Cluster Key
        write!(f, "{}", str::from_utf8(&s).unwrap())
    }
}

impl From<u8> for TypeFb {
    fn from(value: u8) -> Self {
        Self(value)
    }
}
