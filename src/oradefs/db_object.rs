
#[derive(Debug)]
pub struct DataBaseObject {
    schema  : String,
    name    : String,
    options : u8,
}

impl DataBaseObject {
    pub fn new(schema : String, name : String, options : u8) -> Self {
        Self {schema, name, options}
    }
}
