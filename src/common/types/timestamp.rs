use std::fmt::{Formatter, Debug, Display};

#[derive(Default, Copy, Clone)]
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let temp: &dyn Display = self;
        temp.fmt(f)
    }
}

impl Display for TypeTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        
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
        
        write!(f, "{:02}-{:02}-{:04} {:02}:{:02}:{:02}", dd, mm, yy, hh, mi, ss)
    }
}
