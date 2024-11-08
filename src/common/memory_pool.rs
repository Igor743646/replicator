use std::{alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout}, collections::HashSet, fmt::{Display, UpperHex}};
use log::{trace, warn};

use crate::olr_err;
use super::{constants, errors::OLRError};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct MemoryChunk {
    data : *mut u8
}

unsafe impl Sync for MemoryChunk {}
unsafe impl Send for MemoryChunk {}

impl MemoryChunk {
    pub fn new(data : *mut u8) -> Self {
        Self {data}
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, constants::MEMORY_CHUNK_SIZE as usize) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, constants::MEMORY_CHUNK_SIZE as usize) }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data
    }
}

impl Display for MemoryChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.as_slice();

        write!(f, "\n")?;
        for i in data.chunks(64) {
            for j in i.chunks(2) {
                write!(f, "{:02X}{:02X} ", j[0], j[1])?;
            }
            write!(f, "\n")?;
        }
        Ok(())
    }
}

impl UpperHex for MemoryChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}", self.data as usize)
    }
}

#[derive(Debug, Default)]
pub struct MemoryPool {
    memory_min_mb : u64,
    memory_max_mb : u64,
    read_buffer_max : u64,
    memory_chunks_min : u64,
    memory_chunks_max : u64,
    buffers_free : u64,
    buffer_size_max : u64,

    memory_chunks : Vec<MemoryChunk>,
    memory_chunks_allocated_set : HashSet<MemoryChunk>,
    memory_chunks_allocated : u64,
    memory_chunks_free : u64,
    memory_chunks_hmw : u64
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        for chunk in self.memory_chunks_allocated_set.iter() {
            MemoryPool::deallocate_chunk(*chunk);
        }
    }
}

impl MemoryPool {

    const MEMORY_LAYOUT : Layout = unsafe {Layout::from_size_align_unchecked(constants::MEMORY_CHUNK_SIZE as usize, constants::MEMORY_ALIGNMENT as usize)};

    fn allocate_chunk() -> Result<MemoryChunk, OLRError> {
        let memory = unsafe { alloc_zeroed(Self::MEMORY_LAYOUT) };

        if memory.is_null() {
            handle_alloc_error(Self::MEMORY_LAYOUT);
        }
        
        Ok(MemoryChunk::new(memory))
    }

    fn deallocate_chunk(chunk : MemoryChunk) {
        unsafe { dealloc(chunk.data, Self::MEMORY_LAYOUT) };
    }

    pub fn new(memory_min_mb : u64, memory_max_mb : u64, read_buffer_max : u64) -> Result<Self, OLRError> {
        let mut result = Self {
            memory_min_mb,
            memory_max_mb,
            read_buffer_max,
            memory_chunks_min : memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB,
            memory_chunks_max : memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB,
            buffers_free : read_buffer_max,
            buffer_size_max : read_buffer_max * constants::MEMORY_CHUNK_SIZE,
            memory_chunks : Vec::new(),
            memory_chunks_allocated_set : HashSet::new(),
            memory_chunks_allocated : 0,
            memory_chunks_free : 0,
            memory_chunks_hmw : memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB
        };

        for _ in 0 .. result.memory_chunks_min as usize {
            let chunk = MemoryPool::allocate_chunk()?;
            result.memory_chunks_allocated_set.insert(chunk);
            result.memory_chunks.push(chunk);
            result.memory_chunks_allocated += 1;
            result.memory_chunks_free += 1;
        }

        Ok(result)
    }

    pub fn get_chunk(&mut self) -> Result<MemoryChunk, OLRError> {
        if self.memory_chunks_allocated >= self.memory_chunks_max {
            warn!("Memory limit exceeded. The maximum amount of memory available for allocation: {}Mb. Now: {}Mb. Try allocate over limit", self.memory_chunks_max, self.memory_chunks_allocated);
        }

        let return_index = if self.memory_chunks_free == 0 {
            let chunk = MemoryPool::allocate_chunk()?;
            self.memory_chunks_allocated_set.insert(chunk);
            self.memory_chunks.push(chunk);
            self.memory_chunks_allocated += 1;
            (self.memory_chunks_allocated - 1) as usize
        } else {
            self.memory_chunks_free -= 1;
            self.memory_chunks_free as usize
        };

        let result = self.memory_chunks[return_index];
        trace!("Allocate chunk. Address: {:X}. Free/Allocated/Max: {}/{}/{}", 
            result, self.memory_chunks_free, self.memory_chunks_allocated, self.memory_chunks_max );
        
        Ok(result)
    }

    pub fn free_chunk(&mut self, chunk : MemoryChunk) {
        trace!("Free chunk. Address: {:X}. Free/Allocated/Max: {}/{}/{}", 
            chunk, self.memory_chunks_free, self.memory_chunks_allocated, self.memory_chunks_max );

        if self.memory_chunks_free >= self.memory_chunks_min || self.memory_chunks_allocated > self.memory_chunks_max {
            MemoryPool::deallocate_chunk(chunk);
            self.memory_chunks_allocated_set.remove(&chunk);
            self.memory_chunks_allocated -= 1;
        } else {
            self.memory_chunks[self.memory_chunks_free as usize] = chunk;
            self.memory_chunks_free += 1;
        }

    }
}
