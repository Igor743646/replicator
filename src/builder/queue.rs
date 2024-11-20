use std::{collections::VecDeque, fmt::Display, sync::Arc};
use crate::{common::{errors::OLRError, memory_pool::MemoryChunk}, ctx::Ctx};
use log::debug;

#[derive(Debug)]
pub struct BuilderChunk {
    id      : u64,
    size    : usize,
    start   : usize,
    data    : MemoryChunk,
}

impl BuilderChunk {
    pub fn from_mem_chunk(chunk : MemoryChunk) -> Self {
        Self {
            id : 0,
            size : 0,
            start : 0,
            data : chunk,
        }
    }
}

impl Into<MemoryChunk> for BuilderChunk {
    fn into(self) -> MemoryChunk {
        self.data
    }
}

impl Display for BuilderChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BuilderChunk : {{ id: {}, size: {}, start: {}, data: {:?}}}", self.id, self.size, self.start, self.data)
    }
}

#[derive(Debug)]
pub struct BuilderQueue {
    context_ptr : Arc<Ctx>, 
    chunks_allocated : u64,
    queue : VecDeque<BuilderChunk>,
}

impl Drop for BuilderQueue {
    fn drop(&mut self) {
        while let Some(chunk) = self.queue.pop_front() {
            self.context_ptr.free_chunk(chunk.into());
            self.chunks_allocated -= 1;
        }
    }
}

impl BuilderQueue {
    pub fn new(context_ptr : Arc<Ctx>) -> Result<Self, OLRError> {
        debug!("Initialize BuilderQueue");
        
        let chunk: MemoryChunk = context_ptr.get_chunk()?;

        let bchunk = BuilderChunk::from_mem_chunk(chunk);

        Ok(Self {
            context_ptr,
            chunks_allocated : 1,
            queue : VecDeque::from([bchunk]),
        })
    }
}
