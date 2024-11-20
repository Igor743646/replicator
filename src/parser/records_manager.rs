use std::{collections::VecDeque, ptr, sync::Arc};

use crate::{common::{constants, errors::{OLRError, OLRErrorCode::*}, memory_pool::MemoryChunk, types::TypeRecordScn}, ctx::Ctx, olr_err};

use super::{byte_reader::ByteReader, byte_writer::ByteWriter};

#[derive(Debug)]
pub struct Record {
    pub block : u32,
    pub offset : u16,
    pub size : u32,
    pub scn : TypeRecordScn,
    pub sub_scn : u16,
    data : *mut u8,
}

impl Record {
    pub fn data(&mut self) -> &mut [u8] {
        unsafe {
            ptr::slice_from_raw_parts_mut(self.data, self.size as usize).as_mut().unwrap()
        }
    }
}

#[derive(Debug)]
pub struct RecordsManager {
    context_ptr : Arc<Ctx>,

    chunks : VecDeque<MemoryChunk>,
    records : VecDeque<*mut Record>,
}

impl Drop for RecordsManager {
    fn drop(&mut self) {
        self.records.clear();
        while let Some(chunk) = self.chunks.pop_front() {
            self.context_ptr.free_chunk(chunk);
        }
    }
}

impl RecordsManager {
    pub fn new(context_ptr : Arc<Ctx>) -> Self {
        let mut res = Self {
            context_ptr,
            chunks : VecDeque::new(),
            records : VecDeque::new(),
        };

        let mut chunk = res.context_ptr.get_chunk().unwrap();

        let mut writer = ByteWriter::from_bytes(&mut chunk);
        writer.write_u64(size_of::<u64>() as u64).unwrap();

        res.chunks.push_back(chunk);

        res 
    }

    pub fn records_count(&self) -> usize {
        self.records.len()
    }

    fn allocate_chunk(&mut self) -> Result<(), OLRError> {
        let chunk = self.context_ptr.get_chunk()?;
        self.chunks.push_back(chunk);
        Ok(())
    }

    pub fn reserve_record(&mut self, record_size : usize) -> Result<&'static mut Record, OLRError> {
        let mut last_chunk = self.chunks.back_mut().unwrap();

        let mut chunk_size = ByteReader::from_bytes(&last_chunk).read_u64().unwrap();

        if (chunk_size as usize + size_of::<Record>() + record_size + 7) & 0xFFFFFFF8 > constants::MEMORY_CHUNK_SIZE as usize {
            self.allocate_chunk()?;
            last_chunk = self.chunks.back_mut().unwrap();
            chunk_size = size_of::<u64>() as u64;
            ByteWriter::from_bytes(&mut last_chunk).write_u64(chunk_size).unwrap();
        }

        if (chunk_size as usize + size_of::<Record>() + record_size + 7) & 0xFFFFFFF8 > constants::MEMORY_CHUNK_SIZE as usize {
            return olr_err!(MemoryAllocation, "Record is very big for writing in memory chunk of size: {}", constants::MEMORY_CHUNK_SIZE);
        }

        let result = unsafe {
            let record_ptr = (*last_chunk)
                .as_mut_ptr()
                .add(chunk_size as usize) as *mut Record;

            self.records.push_back(record_ptr);

            (*record_ptr).data = (record_ptr as *mut u8).add(size_of::<Record>());

            record_ptr
                .as_mut()
                .unwrap()
        };

        ByteWriter::from_bytes(&mut last_chunk)
            .write_u64(chunk_size + (size_of::<Record>() as u64 + record_size as u64 + 7) & 0xFFFFFFF8).unwrap();

        Ok(result)
    }

    pub fn drop_record(&mut self) -> Option<&'static mut Record> {
        match self.records.pop_front() {
            None => None,
            Some(ptr) => {
                Some(unsafe { ptr.as_mut().unwrap() } )
            }
        }
    }

    pub fn free_chunks(&mut self) {
        self.records.clear();
        
        while self.chunks.len() > 1 {
            let chunk = self.chunks.pop_front();
            self.context_ptr.free_chunk(chunk.unwrap());
        }

        let mut chunk = self.chunks.back_mut().unwrap();
        ByteWriter::from_bytes(&mut chunk).write_u64(size_of::<u64>() as u64).unwrap()
    }

}
