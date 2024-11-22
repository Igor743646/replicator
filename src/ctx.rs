use std::sync::Mutex;

use crossbeam::channel::{Receiver, Sender};
use log::debug;

use crate::{common::{errors::OLRError, memory_pool::{MemoryChunk, MemoryPool}}, parser::fs_reader::ReaderMessage};

#[derive(Debug, Default, Clone)]
pub struct Dump {
    pub level : u64,
    pub path : String,
    pub is_raw : bool,
}

#[derive(Debug)]
pub struct Ctx {
    pub dump : Dump,
    pub log_level : u64,
    pub trace : u64,
    pub flags : u64,
    pub skip_rollback : u64,
    pub disable_checks : u64,

    // State
    pub checkpoint_interval_s : u64,
    pub checkpoint_interval_mb : u64,
    pub checkpoint_keep : u64,
    pub schema_force_interval : u64,

    // Memory Management
    memory_manager : Mutex<MemoryPool>,
}

impl Ctx {
    pub fn new(dump : Dump, log_level : u64, trace : u64, flags : u64, skip_rollback : u64, disable_checks : u64, 
        checkpoint_interval_s : u64, checkpoint_interval_mb : u64, checkpoint_keep : u64,
        schema_force_interval : u64, memory_min_mb: u64 , memory_max_mb: u64, read_buffer_max: u64) -> Result<Self, OLRError> {
        debug!("Initialize Ctx");
        
        Ok(Self {
            dump, log_level : log_level, trace, flags, skip_rollback, disable_checks,
            checkpoint_interval_s, checkpoint_interval_mb, checkpoint_keep,
            schema_force_interval,
            memory_manager : MemoryPool::new(memory_min_mb, memory_max_mb, read_buffer_max)?.into()
        })
    }
    
    pub fn get_chunk(&self) -> Result<MemoryChunk, OLRError> {
        let mut guard = self.memory_manager.lock().unwrap();
        guard.get_chunk()
    }

    pub fn free_chunk(&self, chunk : MemoryChunk) {
        let mut guard = self.memory_manager.lock().unwrap();
        guard.free_chunk(chunk);
    }

    pub fn get_reader_channel(&self) -> (Sender<ReaderMessage>, Receiver<ReaderMessage>)  {
        let guard = self.memory_manager.lock().unwrap();
        crossbeam::channel::bounded::<ReaderMessage>(guard.read_buffer_max() as usize)
    }

    pub fn get_memory_stat(&self) -> String {
        let guard = self.memory_manager.lock().unwrap();
        guard.get_stat_string()
    }
}
