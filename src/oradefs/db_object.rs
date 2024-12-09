use crate::common::constants;

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

    pub fn schema(&self) -> &String {
        &self.schema
    }

    pub fn regexp_name(&self) -> &String {
        &self.name
    }

    pub fn is_system(&self) -> bool {
        self.options & constants::OPTIONS_SYSTEM_TABLE != 0
    } 
}
