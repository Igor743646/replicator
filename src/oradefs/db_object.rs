
#[derive(Debug)]
pub struct DataBaseObject {
    schema  : String,
    name    : String,
    options : u8,
    keys    : Vec<String>,
}

impl DataBaseObject {
    pub fn new(schema : String, name : String, options : u8) -> Self {
        Self {schema, name, options, keys : Vec::new()}
    }

    pub fn add_key(&mut self, key : String) {
        self.keys.push(key);
    }
}
