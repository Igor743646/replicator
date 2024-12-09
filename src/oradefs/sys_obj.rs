use std::collections::HashMap;

use serde::{ser::SerializeStruct, Serialize};

#[derive(Debug, Serialize)]
pub struct SysObj {
    obj : u32,
    data_obj : u32,
    owner : u32,
    name : String,
    obj_type : u16,
    flags : u64,
}

impl SysObj {
    pub fn new(obj : u32, data_obj : u32, owner : u32, name : String, obj_type : u16, flags : u64) -> Self {
        Self {obj, data_obj, owner, name, obj_type, flags}
    }
}

#[derive(Debug, Default, Serialize)]
pub struct SysObjTable {
    rows : HashMap<u32, SysObj>,
}

impl SysObjTable {
    pub fn add_row(&mut self, obj : u32, data_obj : u32, owner : u32, name : String, obj_type : u16, flags : u64) {
        self.rows.insert(obj, SysObj::new(obj, data_obj, owner, name, obj_type, flags));
    }
}