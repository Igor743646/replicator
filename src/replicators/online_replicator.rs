use std::{sync::{mpsc::Sender, Arc, RwLock}, thread::sleep, time};
use log::{debug, info};

use crate::{builder::JsonBuilder, common::{self, errors::OLRError, thread::Thread}, ctx::Ctx, metadata::Metadata, olr_err};
use common::OLRErrorCode::*;

use super::archive_digger::ArchiveDigger;

#[derive(Debug)]
pub struct OnlineReplicator {
    context_ptr     : Arc<RwLock<Ctx>>, 
    builder_ptr     : Arc<RwLock<JsonBuilder>>, 
    metadata_ptr    : Arc<RwLock<Metadata>>,
    
    // Thread info
    alias           : String,

    // Replicator info 
    database_name   : String,
    archive_digger  : Box<dyn ArchiveDigger>,

    // Database connection info
    user            : String, 
    password        : String, 
    server          : String,

    main_channel    : Sender<Result<(), OLRError>>,
} 

impl OnlineReplicator {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>, builder_ptr : Arc<RwLock<JsonBuilder>>, metadata_ptr : Arc<RwLock<Metadata>>, archive_digger  : Box<dyn ArchiveDigger>,
         alias : String, database_name : String, user : String, password : String, server : String, main_channel : Sender<Result<(), OLRError>>) -> Self {
        debug!("Initialize OnlineReplicator");
        Self {
            context_ptr, builder_ptr, metadata_ptr, archive_digger,
            alias, database_name, user, password, server, main_channel
        }
    }
}

impl OnlineReplicator {

}

impl Thread for OnlineReplicator {
    fn run(&self) -> Result<(), OLRError> {
        

        debug!("Stop replicator. Thread: {:?} alias: {}", self.thread_id(), self.alias());
        Ok(())
    }

    fn alias(&self) -> &String {
        &self.alias
    }
}
