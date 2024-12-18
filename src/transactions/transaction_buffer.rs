

use crate::{common::{memory_pool::MemoryChunk, types::{TypeRecordScn, TypeTimestamp}}, olr_perr};

use std::{collections::{hash_map::Entry, HashMap}, sync::Arc};

use crate::{common::{errors::Result, types::TypeXid}, ctx::Ctx, parser::opcodes::Vector};

use super::{transaction::Transaction, transaction_chunk::{TransactionChunk, HEADER_TOTAL_SIZE, TRANSACTION_CHUNK_BUFFER_SIZE}};

#[derive(Debug)]
struct FullnessMarker(usize);

impl FullnessMarker {
    pub(crate) fn new_free() -> Self {
        Self(0xFFFF)
    }

    pub(crate) fn is_free(&self) -> bool {
        self.0 == 0xFFFF
    }

    pub(crate) fn new_occupied() -> Self {
        Self(0)
    }

    pub(crate) fn is_occupied(&self) -> bool {
        self.0 == 0
    }

    pub(crate) fn get_free_position(&self) -> usize {
        self.0.trailing_zeros() as usize
    }

    pub(crate) fn mark_position_occupied(&mut self, pos : usize) {
        self.0 &= !(1 << pos);
    }

    pub(crate) fn mark_position_free(&mut self, pos : usize) {
        self.0 |= 1 << pos;
    }
}


#[derive(Debug)]
struct TransactionMemoryManager {
    context_ptr             : Arc<Ctx>,
    allocated_chunks        : HashMap<*const u8, MemoryChunk>,
    partially_filled_chunks : HashMap<*const u8, FullnessMarker>,
}

impl TransactionMemoryManager {
    pub(crate) fn new(context_ptr : Arc<Ctx>) -> Self {
        Self {
            context_ptr, 
            allocated_chunks : Default::default(),
            partially_filled_chunks : Default::default(),
        }
    }

    pub(crate) fn allocate_memory_chunk(&mut self) -> Result<()> {
        let mem_chunk = self.context_ptr.get_chunk()?;
        let mem_chunk_ptr = mem_chunk.as_ptr();
        let free_mask = FullnessMarker::new_free();

        self.allocated_chunks.insert(mem_chunk_ptr, mem_chunk);
        self.partially_filled_chunks.insert(mem_chunk_ptr, free_mask);
        Ok(())
    }

    pub(crate) fn get_transaction_chunk(&mut self) -> Result<&'static mut TransactionChunk> {
        if self.partially_filled_chunks.is_empty() {
            self.allocate_memory_chunk()?;
        }
        
        let mem_chunk_ptr = self.partially_filled_chunks.keys().next().cloned().unwrap();
        let free_mask = self.partially_filled_chunks.get_mut(&mem_chunk_ptr).unwrap();
        let mem_chunk = self.allocated_chunks.get(&mem_chunk_ptr).unwrap();

        let position = free_mask.get_free_position();
        free_mask.mark_position_occupied(position);

        if free_mask.is_occupied() {
            self.partially_filled_chunks.remove(&mem_chunk_ptr);
        }

        let tr_chunk = unsafe {
            let ptr = mem_chunk.as_ptr() as *mut TransactionChunk;
            ptr.add(position).as_mut().unwrap_unchecked()
        };

        tr_chunk.init(mem_chunk_ptr, position);

        Ok(tr_chunk)
    }

    pub(crate) fn delete_transaction_chunk(&mut self, tr_chunk : &TransactionChunk) -> Result<()> {

        let mem_chunk_ptr = tr_chunk.parent_chunk();
        let position = tr_chunk.position();
        let entry = self.partially_filled_chunks.entry(mem_chunk_ptr);
        
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let free_mask = occupied_entry.get_mut();
                free_mask.mark_position_free(position);
                
                if free_mask.is_free() {
                    occupied_entry.remove();
                    let mem_chunk = self.allocated_chunks.remove(&mem_chunk_ptr).unwrap();
                    self.context_ptr.free_chunk(mem_chunk);
                }
            },
            Entry::Vacant(vacant_entry) => {
                let mut free_mask = FullnessMarker::new_occupied();
                free_mask.mark_position_free(position);
                vacant_entry.insert(free_mask);
            },
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
struct TransactionManager {
    transactions : HashMap<TypeXid, Transaction>,
}

impl TransactionManager {
    pub(crate) fn find_transaction(&mut self, xid : TypeXid, can_add : bool) -> Result<Option<&mut Transaction>> {
        let entry = self.transactions.entry(xid);

        let result = match entry {
            Entry::Occupied(occupied_entry) => {
                Some(occupied_entry.into_mut())
            },
            Entry::Vacant(vacant_entry) => {
                if can_add {
                    Some(vacant_entry.insert(Transaction::new(xid)))
                } else {
                    None
                }
            },
        };

        Ok(result)
    }
}

#[derive(Debug)]
pub struct TransactionBuffer {
    transactions_manager    : TransactionManager,
    memory_manager          : TransactionMemoryManager,
}

unsafe impl Send for TransactionBuffer {}
unsafe impl Sync for TransactionBuffer {}

impl TransactionBuffer {
    pub fn new(context_ptr : Arc<Ctx>) -> Self {
        Self {
            transactions_manager : TransactionManager::default(),
            memory_manager : TransactionMemoryManager::new(context_ptr),
        }
    }

    pub fn init_transaction(&mut self, xid : TypeXid, scn : TypeRecordScn, timestamp : TypeTimestamp) -> Result<()> {
        let transaction = self.transactions_manager.find_transaction(xid, true)?.unwrap();

        transaction.set_start_info(scn, timestamp);

        Ok(())
    }

    pub fn add_double_in_transaction(&mut self, xid : TypeXid, v1 : Vector, v2 : Vector) -> Result<()> {
        let transaction = self.transactions_manager.find_transaction(xid, true)?.unwrap();

        let added_size = v1.size() + v2.size() + HEADER_TOTAL_SIZE;

        if added_size > TRANSACTION_CHUNK_BUFFER_SIZE {
            return olr_perr!("There is no capacity in data buffer for vectors with summary size: {}", added_size);
        }

        if !transaction.has_cappacity_for(added_size) {
            let tr_chunk = self.memory_manager.get_transaction_chunk()?;
            transaction.append_transaction_chunk(tr_chunk);
        }

        transaction.append_double(v1, v2)?;

        Ok(())
    }
}
