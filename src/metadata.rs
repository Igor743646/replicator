use std::{collections::HashSet, sync::{Arc, Mutex, MutexGuard}};

use log::{debug, warn};

use crate::{common::{errors::Result, types::{TypeConId, TypeScn, TypeSeq}}, ctx::Ctx, locales::Locales, oradefs::{db_object::DataBaseObject, oracle_schema::{OracleSchema, OracleSchemaResource}}};

#[derive(Debug)]
pub struct Metadata {
    context_ptr : Arc<Ctx>,
    locales_ptr : Arc<Locales>,
    source_name     : String,
    container_id    : TypeConId,
    start_scn       : TypeScn,
    start_sequence  : TypeSeq,
    start_time      : String,
    start_time_rel  : u64,

    schema_objects : Mutex<Vec<DataBaseObject>>,
    users : Mutex<HashSet<String>>,

    schema : Mutex<OracleSchema>,
}

impl Metadata {
    pub fn new(context_ptr : Arc<Ctx>, locales_ptr : Arc<Locales>,
        source_name     : String,
        container_id    : TypeConId,
        start_scn       : TypeScn,
        start_sequence  : TypeSeq,
        start_time      : String,
        start_time_rel  : u64) -> Self {
        debug!("Initialize Metadata");
        let result = Self {
            context_ptr, locales_ptr, source_name, container_id, start_scn, start_sequence, 
            start_time, start_time_rel, schema_objects : Vec::new().into(), users : HashSet::new().into(),
            schema : Default::default(),
        };
        result
    }

    pub fn add_object<'a>(&'a self, mut user : String, mut table : String, options : u8) -> MutexGuard<'a, Vec<DataBaseObject>> {
        let mut guard = self.schema_objects.lock().unwrap();
        if user.as_bytes().iter().any(|x| u8::is_ascii_lowercase(x)) {
            warn!("In table parameter User: {} not all chars are uppercase. Try force rename.", user);
            user = user.to_ascii_uppercase();
        }

        if table.as_bytes().iter().any(|x| u8::is_ascii_lowercase(x)) {
            warn!("In table parameter Table name: {} not all chars are uppercase. Try force rename.", table);
            table = table.to_ascii_uppercase();
        }

        guard.push(DataBaseObject::new(user, table, options));
        guard
    }

    pub fn add_user(&self, user : String) {
        let mut guard = self.users.lock().unwrap();
        guard.insert(user);
    }

    pub fn set_schema_resource(&self, resource : OracleSchemaResource) -> Result<()> {
        let mut guard = self.schema.lock().unwrap();
        guard.set_resource(resource);
        Ok(())
    }

    pub fn get_schema(&self) -> MutexGuard<'_, OracleSchema> {
        self.schema.lock().unwrap()
    }

}
