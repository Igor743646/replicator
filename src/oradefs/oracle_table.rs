
#[derive(Debug)]
pub struct OracleTable {
    name : String,
}

impl OracleTable {
    pub fn new(name : String) -> Self {
        Self {
            name
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}