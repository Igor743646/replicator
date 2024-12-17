use std::thread::JoinHandle;

use log::debug;

use crate::olr_err;
use crate::common::OLRErrorCode::*;
use super::errors::Result;

pub trait Thread 
    where Self : Send 
{
    fn run(&self) -> Result<()>;
    fn alias(&self) -> String;
    fn thread_id(&self) -> u32 {
        std::process::id()
    }

    fn entry_point(&self) -> Result<()> {
        debug!("Thread id: {} alias: {} started", self.thread_id(), self.alias());
        self.run()
    }
}

pub fn spawn(thread : impl Thread + Send + Sync + 'static) -> Result<JoinHandle<Result<()>>> {
    let alias = thread.alias().to_string();

    let handle = std::thread::Builder::new()
        .name(alias.clone())
        .spawn(move || -> Result<()> {
            thread.entry_point()
        })
        .or_else(|err| olr_err!(ThreadSpawn, "Error while spawn thread {}: {}", alias, err.to_string()))?;

    Ok(handle)
}
