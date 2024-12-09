use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SysTab {
    obj : u32,
    data_obj : u32,
    tablespace : u32,
    clu_cols : u16,
    flags : u64,
    properties : u64,
}

impl SysTab {
    pub fn new(obj : u32, data_obj : u32, tablespace : u32, clu_cols : u16, flags : u64, properties : u64) -> Self {
        Self {obj, data_obj, tablespace, clu_cols, flags, properties}
    }
}

#[derive(Debug, Default, Serialize)]
pub struct SysTabTable {
    rows : HashMap<u32, SysTab>,
}

impl SysTabTable {
    pub fn add_row(&mut self, obj : u32, data_obj : u32, tablespace : u32, clu_cols : u16, flags : u64, properties : u64) {
        self.rows.insert(obj, SysTab::new(obj, data_obj, tablespace, clu_cols, flags, properties));
    }
}