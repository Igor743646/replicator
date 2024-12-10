use std::{fs::OpenOptions, io::Write, sync::{Arc, Mutex}};
use log::{debug, warn};

use formats::BuilderFormats;
use queue::BuilderQueue;
use serde_json::json;

use crate::{common::{errors::OLRError, types::{TypeRecordScn, TypeTimestamp, TypeXid}}, ctx::Ctx, locales::Locales, metadata::Metadata, parser::opcodes::{opcode0501::OpCode0501, opcode1102::OpCode1102}};

pub mod formats;
pub mod queue;

#[derive(Debug)]
pub struct JsonBuilder {
    context_ptr : Arc<Ctx>, 
    locales_ptr : Arc<Locales>, 
    metadata_ptr : Arc<Metadata>, 
    formats : BuilderFormats,
    queue : Mutex<BuilderQueue>,
}

impl JsonBuilder {
    pub fn new(context_ptr : Arc<Ctx>, locales_ptr : Arc<Locales>, metadata_ptr : Arc<Metadata>, 
                db_format : u8, attributes_format : u8, interval_dts_format : u8, interval_ytm_format : u8, message_format : u8, 
                rid_format : u8, xid_format : u8, timestamp_format : u8, timestamp_tz_format : u8, timestamp_all : u8, char_format : u8,
                scn_format : u8, scn_all : u8, unknown_format : u8, schema_format : u8, column_format : u8, unknown_type : u8) -> Result<Self, OLRError> {
        debug!("Initialize JsonBuilder");
        let _ = OpenOptions::new().write(true).create(true).truncate(true).open("out.txt").unwrap(); // Убрать
        Ok(Self {
            context_ptr : context_ptr.clone(), locales_ptr, metadata_ptr, 
            formats : BuilderFormats {
                db_format, 
                attributes_format, 
                interval_dts_format, 
                interval_ytm_format, 
                message_format, 
                rid_format, 
                xid_format, 
                timestamp_format, 
                timestamp_tz_format, 
                timestamp_all, 
                char_format, 
                scn_format, 
                scn_all, 
                unknown_format, 
                schema_format, 
                column_format, 
                unknown_type,
            },
            queue : Mutex::new(BuilderQueue::new(context_ptr)?),
        })
    }

    pub fn process_insert(&self, scn : TypeRecordScn, timestamp : TypeTimestamp, undo : &OpCode0501, redo : &OpCode1102) -> Result<(), OLRError> {

        let mut schema = self.metadata_ptr.get_schema();

        let mut guard = self.queue.lock().unwrap();
        
        let mut output_file = OpenOptions::new().write(true).append(true).open("out.txt").unwrap();

        let table = schema.get_table(undo.obj)?;

        if table.is_none() {
            warn!("No table with obj_id: {}", undo.obj);
            return Ok(());
        }

        let table = table.unwrap();

        let value = json!({
            "OP" : "insert",
            "SCN": scn.to_string(),
            "TIMESTAMP": timestamp.to_string(),
            "XID": undo.xid.to_string(),
            "TABLE": table.name(),
            "DATA_OBJ": undo.data_obj,
        });

        output_file.write(value.to_string().as_bytes()).unwrap();
        output_file.write(b"\n").unwrap();

        Ok(())
    }

    pub fn process_begin(&self, scn : TypeRecordScn, timestamp : TypeTimestamp, xid : TypeXid) -> Result<(), OLRError> {
        let mut guard = self.queue.lock().unwrap();
        
        let mut output_file = OpenOptions::new().write(true).append(true).open("out.txt").unwrap();

        let value = json!({
            "OP" : "start",
            "SCN": scn.to_string(),
            "TIMESTAMP": timestamp.to_string(),
            "XID": xid.to_string(),
        });

        output_file.write(value.to_string().as_bytes()).unwrap();
        output_file.write(b"\n").unwrap();

        Ok(())
    }

    pub fn process_commit(&self, scn : TypeRecordScn, timestamp : TypeTimestamp, xid : TypeXid, is_rollback : bool) -> Result<(), OLRError> {
        let mut guard = self.queue.lock().unwrap();
        
        let mut output_file = OpenOptions::new().write(true).append(true).open("out.txt").unwrap();

        let value = json!({
            "OP" : if is_rollback {"rollback"} else {"commit"},
            "SCN": scn.to_string(),
            "TIMESTAMP": timestamp.to_string(),
            "XID": xid.to_string(),
        });

        output_file.write(value.to_string().as_bytes()).unwrap();
        output_file.write(b"\n").unwrap();

        Ok(())
    }
}
