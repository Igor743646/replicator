

use std::collections::VecDeque;

use crate::{common::types::{TypeRecordScn, TypeTimestamp, TypeXid}, olr_err, parser::opcodes::Vector};

use super::transaction_chunk::{TransactionChunk, TRANSACTION_CHUNK_BUFFER_SIZE};
use crate::common::errors::Result;
use crate::common::OLRErrorCode::TransactionMemory;

#[derive(Debug)]
pub struct Transaction {
    xid : TypeXid,

    is_begined : bool,
    scn : Option<TypeRecordScn>,
    timestamp : Option<TypeTimestamp>,

    chunks : VecDeque<&'static mut TransactionChunk>,
}

impl Transaction {
    pub fn new(xid : TypeXid) -> Self {
        Self {
            xid,
            scn : None,
            timestamp : None,
            is_begined : false,
            chunks : VecDeque::new(),
        }
    }

    pub fn set_start_info(&mut self, scn : TypeRecordScn, timestamp : TypeTimestamp) {
        self.is_begined = true;
        self.scn = Some(scn);
        self.timestamp = Some(timestamp)
    }

    pub fn has_cappacity_for(&self, size : usize) -> bool {
        match self.chunks.back() {
            Some(chunk) => chunk.size() + size <= TRANSACTION_CHUNK_BUFFER_SIZE,
            None => false
        }
    }

    pub fn append_transaction_chunk(&mut self, chunk : &'static mut TransactionChunk) {
        match self.chunks.back_mut() {
            Some(last) => {
                last.set_next(chunk);
                chunk.set_prev(*last);
            },
            None => (),
        }

        self.chunks.push_back(chunk);
    }

    pub fn append_double(&mut self, v1 : Vector, v2 : Vector) -> Result<()> {
        match self.chunks.back_mut() {
            Some(last) => {
                last.append_double(v1, v2)?;
                Ok(())
            },
            None => olr_err!(TransactionMemory, "No chunk for pushing vectors"),
        }
    }
}
