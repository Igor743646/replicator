use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SysUser {
    user : u32,
    name : String,
    spare1 : u128,
}

impl SysUser {
    pub fn new(user : u32, name : String, spare1 : u128) -> Self {
        Self {user, name, spare1}
    }
}

#[derive(Debug, Default, Serialize)]
pub struct SysUserTable {
    rows : HashMap<u32, SysUser>,
}

impl SysUserTable {
    pub fn add_row(&mut self, user : u32, name : String, spare1 : u128) {
        self.rows.insert(user, SysUser::new(user, name, spare1));
    }
}