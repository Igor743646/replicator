use std::{borrow::BorrowMut, fmt::{Debug, Display}, io::Write, ops::Deref, ptr::{write, write_unaligned, NonNull}, sync::{Arc, RwLock}};
use crate::{common::{errors::OLRError, mem_manager::MemoryChunk}, ctx::Ctx};

use bytebuffer::{ByteReader, ByteBuffer};
use log::{info, debug};

pub struct BuilderChunk {
    id      : u64,
    size    : usize,
    start   : usize,
    data    : *mut u8,
    next    : *mut BuilderChunk,
}

impl BuilderChunk {
    pub fn from_mem_chunk(mut chunk : MemoryChunk) -> &'static mut Self {
        unsafe {
            let result = (chunk.as_mut_ptr() as *mut BuilderChunk).as_mut().unwrap();
            result.data = chunk.as_mut_ptr().add(size_of::<BuilderChunk>());
            result
        }
    }

    pub fn as_mem_chunk(&self) -> MemoryChunk {
        MemoryChunk::new(self as *const BuilderChunk as *mut u8)
    }
}

impl Display for BuilderChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BuilderChunk : {{ id: {}, size: {}, start: {}, data: {:?}, next: {:?}}}", self.id, self.size, self.size, self.data, self.next)
    }
}

#[derive(Default)]
pub struct BuilderQueue<'a> {
    context_ptr : Arc<RwLock<Ctx>>, 
    chunks_allocated : u64,
    first_chunk : Option<&'a BuilderChunk>,
    last_chunk : Option<&'a BuilderChunk>,
}

impl<'a> BuilderQueue<'a> {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>) -> Result<Self, OLRError> {
        info!("Initialize BuilderQueue");
        
        let chunk = {
            let mut context  = context_ptr.write().unwrap();
            context.get_chunk()?
        };

        let bchunk = BuilderChunk::from_mem_chunk(chunk);

        Ok(Self {
            context_ptr,
            chunks_allocated : 1,
            first_chunk : Some(bchunk), 
            last_chunk : Some(bchunk),
        })
    }
}
