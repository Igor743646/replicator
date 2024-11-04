use std::alloc::{alloc_zeroed, handle_alloc_error, Layout};

use log::debug;

use crate::{constants, errors::OracleDBReplicatorError};


#[derive(Debug, Default)]
pub struct Dump {
    pub level : u64,
    pub path : String,
    pub is_raw : bool,
}

#[derive(Debug, Default)]
pub struct Ctx<'a> {
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
    memory_min_mb : u64,
    memory_max_mb : u64,
    read_buffer_max : u64,
    memory_chunks_min : u64,
    memory_chunks_max : u64,
    buffers_free : u64,
    buffer_size_max : u64,

    memory_chunks : Vec<&'a [u8]>,
    memory_chunks_allocated : u64,
    memory_chunks_free : u64,
    memory_chunks_hmw : u64,
}

impl<'a> Ctx<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn allocate_chunk(&self) -> Result<&'a [u8], OracleDBReplicatorError> {
        let size = constants::MEMORY_CHUNK_SIZE as usize;
        let alignment = constants::MEMORY_ALIGNMENT as usize;

        let layout = Layout::from_size_align(size, alignment)
                .or(OracleDBReplicatorError::new(020001, format!("Problem with layout construct: size: {} align: {}", 
                    size, alignment)).err())?;

        unsafe {
            let memory = alloc_zeroed(layout);

            if memory.is_null() {
                handle_alloc_error(layout);
            }

            let r = core::ptr::slice_from_raw_parts_mut(memory, size);
            Ok(r.as_ref().unwrap())
        }
    }

    pub fn initialize(&mut self, memory_min_mb : u64, memory_max_mb : u64, read_buffer_max : u64) -> Result<(), OracleDBReplicatorError> {
        debug!("Initialize Ctx: memory_min_mb: {} memory_max_mb: {} read_buffer_max: {}", memory_min_mb, memory_max_mb, read_buffer_max);
        self.memory_min_mb = memory_min_mb;
        self.memory_max_mb = memory_max_mb;
        self.memory_chunks_min = memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB;
        self.memory_chunks_max = memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB;
        self.read_buffer_max = read_buffer_max;
        self.buffers_free = read_buffer_max;
        self.buffer_size_max = read_buffer_max * constants::MEMORY_CHUNK_SIZE;

        self.memory_chunks = Vec::new();
        for _ in 0..self.memory_chunks_min as usize {
            self.memory_chunks.push(self.allocate_chunk()?);

            self.memory_chunks_allocated += 1;
            self.memory_chunks_free += 1;
        }
        self.memory_chunks_hmw = self.memory_chunks_min;

        Ok(())
    }
}
