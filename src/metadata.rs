use std::{collections::HashSet, sync::{Arc, RwLock}};

use log::{debug, warn};

use crate::{common::{constants, types::{TypeConId, TypeScn, TypeSeq}}, ctx::Ctx, locales::Locales, oradefs::db_object::DataBaseObject};

#[derive(Debug)]
pub struct Metadata {
    context_ptr : Arc<RwLock<Ctx>>,
    locales_ptr : Arc<RwLock<Locales>>,
    source_name     : String,
    container_id    : TypeConId,
    start_scn       : TypeScn,
    start_sequence  : TypeSeq,
    start_time      : String,
    start_time_rel  : u64,

    schema_objects : Vec<DataBaseObject>,
    users : HashSet<String>,
}

impl Metadata {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>, locales_ptr : Arc<RwLock<Locales>>,
        source_name     : String,
        container_id    : TypeConId,
        start_scn       : TypeScn,
        start_sequence  : TypeSeq,
        start_time      : String,
        start_time_rel  : u64) -> Self {
        debug!("Initialize Metadata");
        let mut result = Self {
            context_ptr, locales_ptr, source_name, container_id, start_scn, start_sequence, 
            start_time, start_time_rel, schema_objects : Vec::new(), users : HashSet::new(),
        };
        result.reset_objects();
        result
    }

    pub fn reset_objects(&mut self) {
        self.schema_objects.clear();

        self.schema_objects.push(DataBaseObject::new("SYS".into(), "CCOL\\$".into(), constants::OPTIONS_SYSTEM_TABLE | constants::OPTIONS_SCHEMA_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "CDEF\\$".into(), constants::OPTIONS_SYSTEM_TABLE | constants::OPTIONS_SCHEMA_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "COL\\$".into(), constants::OPTIONS_SYSTEM_TABLE | constants::OPTIONS_SCHEMA_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "DEFERRED_STG\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "ECOL\\$".into(), constants::OPTIONS_SYSTEM_TABLE | constants::OPTIONS_SCHEMA_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "LOB\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "LOBCOMPPART\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "LOBFRAG\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "OBJ\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "TAB\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "TABPART\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "TABCOMPART\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "TABSUBPART\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "TS\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("SYS".into(), "USER\\$".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("XDB".into(), "XDB\\$TTSET".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("XDB".into(), "X\\$NM.*".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("XDB".into(), "X\\$PT.*".into(), constants::OPTIONS_SYSTEM_TABLE));
        self.schema_objects.push(DataBaseObject::new("XDB".into(), "X\\$QN.*".into(), constants::OPTIONS_SYSTEM_TABLE));
    }

    pub fn add_object(&mut self, mut user : String, mut table : String, options : u8) -> &mut DataBaseObject {
        if user.as_bytes().iter().any(|x| u8::is_ascii_lowercase(x)) {
            warn!("In table parameter User: {} not all chars are uppercase. Try force rename.", user);
            user = user.to_ascii_uppercase();
        }

        if table.as_bytes().iter().any(|x| u8::is_ascii_lowercase(x)) {
            warn!("In table parameter Table name: {} not all chars are uppercase. Try force rename.", table);
            table = table.to_ascii_uppercase();
        }

        self.schema_objects.push(DataBaseObject::new(user, table, options));
        unsafe { self.schema_objects.last_mut().unwrap_unchecked() } 
    }

    pub fn add_user(&mut self, user : String) {
        self.users.insert(user);
    }
}
