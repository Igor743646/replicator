use std::sync::Arc;

use log::debug;

use crate::common::{errors::OLRError, mem_manager::{MemoryChunk, MemoryManager}};

#[derive(Debug, Default)]
pub struct Dump {
    pub level : u64,
    pub path : String,
    pub is_raw : bool,
}

#[derive(Debug, Default)]
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
    memory_manager : MemoryManager,
}

impl Ctx {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn initialize(&mut self, memory_min_mb : u64, memory_max_mb : u64, read_buffer_max : u64) -> Result<(), OLRError> {
        debug!("Initialize Ctx: memory_min_mb: {} memory_max_mb: {} read_buffer_max: {}", memory_min_mb, memory_max_mb, read_buffer_max);

        self.memory_manager = MemoryManager::new(memory_min_mb, memory_max_mb, read_buffer_max)?;

        Ok(())
    }

    pub fn get_chunk(&mut self) -> Result<MemoryChunk, OLRError> {
        self.memory_manager.get_chunk()
    }

    pub fn free_chunk(&mut self, chunk : MemoryChunk) {
        self.memory_manager.free_chunk(chunk);
    }
}
