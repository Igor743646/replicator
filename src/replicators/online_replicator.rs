use std::{cmp::Reverse, sync::{Arc, Mutex}};
use log::{debug, info, warn};

use crate::{builder::JsonBuilder, common::{errors::Result, thread::Thread}, ctx::Ctx, metadata::Metadata, olr_err, oradefs::oracle_schema::OracleSchemaResource, transactions::transaction_buffer::TransactionBuffer};

use super::archive_digger::ArchiveDigger;
use crate::common::OLRErrorCode::*;

#[derive(Debug)]
pub struct OnlineReplicator {
    context_ptr     : Arc<Ctx>, 
    builder_ptr     : Arc<JsonBuilder>, 
    metadata_ptr    : Arc<Metadata>,
    
    // Thread info
    alias           : String,

    // Replicator info 
    database_name   : String,
    archive_digger  : Box<dyn ArchiveDigger>,
    transaction_buffer : Arc<Mutex<TransactionBuffer>>,

    // Database connection info
    user            : String,
    password        : String,
    server          : String,
} 

impl OnlineReplicator {
    pub fn new(context_ptr : Arc<Ctx>, builder_ptr : Arc<JsonBuilder>, metadata_ptr : Arc<Metadata>, archive_digger  : Box<dyn ArchiveDigger>,
         alias : String, database_name : String, user : String, password : String, server : String) -> Self {
        debug!("Initialize OnlineReplicator");
        Self {
            context_ptr : context_ptr.clone(), builder_ptr, metadata_ptr, archive_digger, 
            transaction_buffer : Arc::new(Mutex::new(TransactionBuffer::new(context_ptr))),
            alias, database_name, user, password, server
        }
    }
}

impl Thread for OnlineReplicator {
    fn run(&self) -> Result<()> {
        info!("Run Replicator");

        let conn = oracle::Connection::connect(&self.user, &self.password, &self.server)
            .map_err(|err| olr_err!(OracleConnection, "Problems with connection: {}", err))?;
        
        self.metadata_ptr.set_schema_resource(OracleSchemaResource::FromConnection(conn))?;

        let mut parsers_queue = self.archive_digger.get_parsers_queue(self.transaction_buffer.clone())?;
        
        while let Some(Reverse(mut parser)) = parsers_queue.pop() {
            debug!("Parse sequence: {}", parser.sequence());

            let res = parser.parse();

            if res.is_err() {
                warn!("Can not parse sequence: {}. Stop replication", parser.sequence());
                return res;
            }
        }

        info!("Stop replicator. Thread id: {} alias: {}", self.thread_id(), self.alias());
        info!("{}", self.context_ptr.get_memory_stat());
        Ok(())
    }

    fn alias(&self) -> String {
        self.alias.clone()
    }
}
