use log::info;

use crate::parser::opcodes::Vector;
use crate::common::errors::Result;

#[derive(Debug)]
pub struct TransactionChunkHeader {
    parent_mem : *const u8,
    position : usize,
    size : usize,
    elements : usize,
    prev : Option<*mut TransactionChunk>,
    next : Option<*mut TransactionChunk>,
}

pub const TRANSACTION_CHUNK_SIZE : usize = 64 * 1024;
pub const TRANSACTION_CHUNK_HEADER_SIZE : usize = size_of::<TransactionChunkHeader>();
pub const TRANSACTION_CHUNK_BUFFER_SIZE : usize = TRANSACTION_CHUNK_SIZE - TRANSACTION_CHUNK_HEADER_SIZE;
pub const HEADER_OFFSET_OP : usize = 0;
pub const HEADER_OFFSET_VECTOR1 : usize = size_of::<u32>();
pub const HEADER_OFFSET_VECTOR2 : usize = size_of::<u32>() + size_of::<Vector>();
pub const HEADER_OFFSET_DATA : usize = size_of::<u32>() + size_of::<Vector>() + size_of::<Vector>();
pub const HEADER_OFFSET_DATA_SIZE : usize = size_of::<u32>() + size_of::<Vector>() + size_of::<Vector>();
pub const HEADER_TOTAL_SIZE : usize = size_of::<u32>() + size_of::<Vector>() + size_of::<Vector>() + size_of::<usize>();

#[derive(Debug)]
pub struct TransactionChunk {
    header : TransactionChunkHeader,
    buffer : [u8; TRANSACTION_CHUNK_BUFFER_SIZE],
}

unsafe impl Send for TransactionChunk {}
unsafe impl Sync for TransactionChunk {}

impl TransactionChunk {
    pub(crate) fn init(&mut self, parent_mem : *const u8, position : usize) {
        self.header.parent_mem = parent_mem;
        self.header.position = position;
        self.header.size = 0;
        self.header.elements = 0;
        self.header.prev = None;
        self.header.next = None;
    }

    pub(crate) fn parent_chunk(&self) -> *const u8 {
        self.header.parent_mem
    }

    pub(crate) fn position(&self) -> usize {
        self.header.position
    }

    pub(crate) fn size(&self) -> usize {
        self.header.size
    }

    pub(crate) fn set_prev(&mut self, chunk : *mut TransactionChunk) {
        self.header.prev = Some(chunk);
    }

    pub(crate) fn set_next(&mut self, chunk : *mut TransactionChunk) {
        self.header.next = Some(chunk);
    }
    
    pub(crate) fn begin(&mut self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }

    pub(crate) fn end(&mut self) -> *mut u8 {
        unsafe { self.buffer.as_mut_ptr().add(self.size()) }
    }

    pub(crate) fn append_double(&mut self, v1 : Vector, v2 : Vector) -> Result<()> {
        let opcodes: u32 = ((v1.opcode() as u32) << 16) | (v2.opcode() as u32);
        let v1_data = v1.data();
        let v2_data = v2.data();
        let v1_size = v1.size();
        let v2_size = v2.size();
        let size = v1_size + v2_size + HEADER_TOTAL_SIZE;
        
        unsafe {
            std::ptr::write((self.end().add(HEADER_OFFSET_OP)) as *mut u32, opcodes);
            std::ptr::write((self.end().add(HEADER_OFFSET_VECTOR1)) as *mut Vector, v1);
            std::ptr::write((self.end().add(HEADER_OFFSET_VECTOR2)) as *mut Vector, v2);
            std::ptr::copy_nonoverlapping(v1_data, self.end().add(HEADER_OFFSET_DATA), v1_size);
            std::ptr::copy_nonoverlapping(v2_data, self.end().add(HEADER_OFFSET_DATA + v1_size), v2_size);
            std::ptr::write((self.end().add(HEADER_OFFSET_DATA_SIZE)) as *mut usize, size);
        }

        self.header.elements += 1;
        self.header.size += size;

        Ok(())
    }
}
