use std::{cmp::Reverse, sync::Arc};
use log::{debug, warn};

use crate::{builder::JsonBuilder, common::{errors::OLRError, thread::Thread}, ctx::Ctx, metadata::Metadata};

use super::archive_digger::ArchiveDigger;

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
            context_ptr, builder_ptr, metadata_ptr, archive_digger,
            alias, database_name, user, password, server
        }
    }
}

impl OnlineReplicator {

}

impl Thread for OnlineReplicator {
    fn run(&self) -> Result<(), OLRError> {
        
        let mut parsers_queue = self.archive_digger.get_parsers_queue().unwrap();
        
        while let Some(Reverse(mut parser)) = parsers_queue.pop() {
            debug!("Parse sequence: {}", parser.sequence());

            let res = parser.parse();

            if let Err(error) = res {
                warn!("Can not parse sequence: {}. Stop replication", parser.sequence());
                return error.into();
            }
        }

        debug!("Stop replicator. Thread id: {} alias: {}", self.thread_id(), self.alias());
        Ok(())
    }

    fn alias(&self) -> String {
        self.alias.clone()
    }
}
