use std::{alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout}, collections::VecDeque, fmt::{Display, UpperHex}, ptr::NonNull};
use log::{trace, warn};

use crate::olr_err;
use super::{constants, errors::OLRError};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MemoryChunk {
    data : NonNull<[u8]>,
}

unsafe impl Sync for MemoryChunk {}
unsafe impl Send for MemoryChunk {}

impl MemoryChunk {
    pub const MEMORY_CHUNK_SIZE : usize = constants::MEMORY_CHUNK_SIZE as usize;
    pub const MEMORY_ALIGNMENT : usize = constants::MEMORY_ALIGNMENT as usize;
    pub const MEMORY_LAYOUT : Layout = unsafe {Layout::from_size_align_unchecked(Self::MEMORY_CHUNK_SIZE, Self::MEMORY_ALIGNMENT)};

    pub fn new() -> Self {
        let data_ptr : NonNull<[u8]>;

        unsafe {
            let memory = alloc_zeroed(Self::MEMORY_LAYOUT) ;

            if memory.is_null() {
                handle_alloc_error(Self::MEMORY_LAYOUT);
            }

            data_ptr = NonNull::new_unchecked(std::ptr::slice_from_raw_parts_mut(memory, Self::MEMORY_LAYOUT.size()));
        }

        Self {
            data : data_ptr
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.data.as_mut() }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { self.data.as_ref() }
    }

    pub fn as_mut_ptr(&mut self) -> *mut [u8] {
        self.data.as_ptr()
    }

    pub fn as_raw_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr() as *mut u8
    }

    pub fn as_ptr(&self) -> *const [u8] {
        self.data.as_ptr()
    }

    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as *const u8
    }
}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.as_mut_ptr() as *mut u8, Self::MEMORY_LAYOUT);
        }
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
        write!(f, "{:X}", self.data.as_ptr() as *const u8 as usize)
    }
}

#[derive(Debug)]
pub struct MemoryPool {
    memory_min_mb : u64,
    memory_max_mb : u64,
    read_buffer_max : u64,
    memory_chunks_min : u64,
    memory_chunks_max : u64,
    buffers_free : u64,
    buffer_size_max : u64,

    memory_chunks : VecDeque<MemoryChunk>,
    memory_chunks_allocated : u64,
    memory_chunks_free : u64,
    memory_chunks_hmw : u64
}

impl MemoryPool {
    pub fn new(memory_min_mb : u64, memory_max_mb : u64, read_buffer_max : u64) -> Result<Self, OLRError> {
        let mut result = Self {
            memory_min_mb,
            memory_max_mb,
            read_buffer_max,
            memory_chunks_min : memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB,
            memory_chunks_max : memory_max_mb / constants::MEMORY_CHUNK_SIZE_MB,
            buffers_free : read_buffer_max,
            buffer_size_max : read_buffer_max * constants::MEMORY_CHUNK_SIZE,
            memory_chunks : VecDeque::with_capacity((memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB) as usize),
            memory_chunks_allocated : 0,
            memory_chunks_free : 0,
            memory_chunks_hmw : memory_min_mb / constants::MEMORY_CHUNK_SIZE_MB
        };

        for _ in 0 .. result.memory_chunks_min as usize {
            let chunk = MemoryChunk::new();
            result.memory_chunks.push_back(chunk);
            result.memory_chunks_allocated += 1;
            result.memory_chunks_free += 1;
        }

        Ok(result)
    }

    pub fn get_chunk(&mut self) -> Result<MemoryChunk, OLRError> {
        if self.memory_chunks_allocated >= self.memory_chunks_max {
            warn!("Memory limit exceeded. The maximum amount of memory available for allocation: {}Mb. Now: {}Mb. Try allocate over limit", self.memory_chunks_max, self.memory_chunks_allocated);
        }

        if self.memory_chunks_free == 0 {
            let chunk = MemoryChunk::new();
            self.memory_chunks.push_back(chunk);
            self.memory_chunks_allocated += 1;
            (self.memory_chunks_allocated - 1) as usize
        } else {
            self.memory_chunks_free -= 1;
            self.memory_chunks_free as usize
        };

        let result = self.memory_chunks.pop_front().unwrap();
        trace!("Allocate chunk. Address: {:X}. Free/Allocated/Max: {}/{}/{}", 
            result.as_raw_ptr() as usize, self.memory_chunks_free, self.memory_chunks_allocated, self.memory_chunks_max );
        
        Ok(result)
    }

    pub fn free_chunk(&mut self, chunk : MemoryChunk) {
        trace!("Free chunk. Address: {:0X}. Free/Allocated/Max: {}/{}/{}", 
            chunk.as_raw_ptr() as usize, self.memory_chunks_free, self.memory_chunks_allocated, self.memory_chunks_max );

        if self.memory_chunks_free >= self.memory_chunks_min || self.memory_chunks_allocated > self.memory_chunks_max {
            let _ = chunk;
            self.memory_chunks_allocated -= 1;
        } else {
            self.memory_chunks[self.memory_chunks_free as usize] = chunk;
            self.memory_chunks_free += 1;
        }

    }
}
