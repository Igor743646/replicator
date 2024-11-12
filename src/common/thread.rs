use std::any::Any;
use std::panic::UnwindSafe;
use std::thread::{JoinHandle, ThreadId};

use log::debug;

use crate::olr_err;
use crate::common::OLRErrorCode::*;
use super::errors::OLRError;

pub trait Thread 
    where Self : Send 
{
    fn run(&self) -> Result<(), OLRError>;
    fn alias(&self) -> &String;
    fn thread_id(&self) -> ThreadId {
        std::thread::current().id()
    }

    fn entry_point(&self) -> Result<(), OLRError> {
        debug!("Thread id: {:?} alias: {} started", self.thread_id(), self.alias());
        self.run()
    }
}

pub fn spawn(thread : Box<dyn Thread + Send + Sync>) -> Result<JoinHandle<Result<(), OLRError>>, OLRError> {
    let alias = thread.alias().to_string();

    let handle = std::thread::Builder::new()
        .name(alias.clone())
        .spawn(move || -> Result<(), OLRError> {
            thread.entry_point()
        })
        .or_else(|err| olr_err!(ThreadSpawn, "Error while spawn thread {}: {}", alias, err.to_string()).into())?;

    Ok(handle)
}
