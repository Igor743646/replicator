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
    archive_digger  : Box<dyn ArchiveDigger>,
    alias           : String, 
    name            : String, 
    user            : String, 
    password        : String, 
    server          : String,

    main_channel    : Sender<Result<(), OLRError>>,
} 

impl OnlineReplicator {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>, builder_ptr : Arc<RwLock<JsonBuilder>>, metadata_ptr : Arc<RwLock<Metadata>>, archive_digger  : Box<dyn ArchiveDigger>,
         alias : String, name : String, user : String, password : String, server : String, main_channel : Sender<Result<(), OLRError>>) -> Self {
        debug!("Initialize OnlineReplicator");
        Self {
            context_ptr, builder_ptr, metadata_ptr, archive_digger,
            alias, name, user, password, server, main_channel
        }
    }
}

impl Thread for OnlineReplicator {
    fn run(&self) -> Result<(), OLRError> {
        let mut cycles = 0;
        loop {
            info!("Work");
            cycles += 1;
            sleep(time::Duration::from_millis(1000));
            if cycles > 4 {
                break;
            }
        }

        self.main_channel
            .send(Ok(()))
            .or_else(|err| 
                olr_err!(ChannelSend, "Send error while stopping thread {}: {}", self.alias, err.to_string()).into()
            )?;

        Ok(())
    }

    fn alias(&self) -> &String {
        &self.alias
    }
}
