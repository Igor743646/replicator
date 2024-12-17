use std::{collections::VecDeque, sync::{Arc, Mutex, MutexGuard}};

use crate::{common::{errors::OLRError, types::{TypeRecordScn, TypeScn, TypeTimestamp, TypeXid}}, parser::opcodes::VectorData};

use super::transaction_buffer::TransactionBuffer;



#[derive(Debug)]
pub struct Transaction {
    xid : TypeXid,

    is_begined : bool,
    scn : Option<TypeRecordScn>,
    timestamp : Option<TypeTimestamp>,
}

impl Transaction {
    pub fn new(xid : TypeXid) -> Self {
        Self {
            xid,
            scn : None,
            timestamp : None,
            is_begined : false,
        }
    }

    pub fn set_start_info(&mut self, scn : TypeRecordScn, timestamp : TypeTimestamp) {
        self.is_begined = true;
        self.scn = Some(scn);
        self.timestamp = Some(timestamp)
    }

    pub fn add_double(&mut self, buffer : &mut MutexGuard<'_, TransactionBuffer>, vector1 : VectorData, vector2 : VectorData) -> Result<(), OLRError> {
        Ok(())
    }
}
