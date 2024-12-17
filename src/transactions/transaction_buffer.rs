use std::{collections::{hash_map::Entry, HashMap}, sync::{Arc, Mutex, MutexGuard}};

use crate::{common::{errors::Result, types::{TypeScn, TypeTimestamp, TypeXid}}, ctx::Ctx, olr_err, parser::opcodes::{Vector, VectorData}};
use crate::common::errors::OLRErrorCode::*;

use super::transaction::Transaction;

#[derive(Debug)]
pub struct TransactionBuffer {
    context_ptr     : Arc<Ctx>,
    transactions    : HashMap<TypeXid, Transaction>,
}

impl TransactionBuffer {
    pub fn new(context_ptr : Arc<Ctx>) -> Self {
        Self { 
            context_ptr,
            transactions : HashMap::new()
        }
    }

    pub fn find_transaction(&mut self, xid : TypeXid, can_add : bool) -> Result<Option<&mut Transaction>> {
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

    pub fn add_double_in_transaction(&mut self, xid : TypeXid, v1 : Vector, v2 : Vector) -> Result<()> {
        let transaction = self.find_transaction(xid, true)?.unwrap();

        // let added_size = 

        Ok(())
    }
}
