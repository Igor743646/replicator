use std::{collections::VecDeque, fmt::Display, sync::{Arc, RwLock}};
use crate::{common::{errors::OLRError, memory_pool::MemoryChunk}, ctx::Ctx, olr_err};
use crate::common::OLRErrorCode::*;
use log::info;

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
    context_ptr : Arc<RwLock<Ctx>>, 
    chunks_allocated : u64,
    queue : VecDeque<BuilderChunk>,
}

impl BuilderQueue {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>) -> Result<Self, OLRError> {
        info!("Initialize BuilderQueue");
        
        let chunk: MemoryChunk = {
            let mut context  = context_ptr.write()
                                                                     .or(olr_err!(TakeLock, "Error with other thread").into())?;
            context.get_chunk()?
        };

        let bchunk = BuilderChunk::from_mem_chunk(chunk);

        Ok(Self {
            context_ptr,
            chunks_allocated : 1,
            queue : VecDeque::from([bchunk]),
        })
    }
}
