use std::sync::{Arc, RwLock};

use crate::{ctx::Ctx, locales::Locales, types::{TypeConId, TypeScn, TypeSeq}};


#[derive(Debug)]
pub struct Metadata<'a> {
    context : Arc<RwLock<Ctx<'a>>>,
    locales : Arc<RwLock<Locales>>,
    source_name     : String,
    container_id    : TypeConId,
    start_scn       : TypeScn,
    start_sequence  : TypeSeq,
    start_time      : String,
    start_time_rel  : u64,
}

impl<'a> Metadata<'a> {
    pub fn new(context : Arc<RwLock<Ctx<'a>>>, locales : Arc<RwLock<Locales>>,
        source_name     : String,
        container_id    : TypeConId,
        start_scn       : TypeScn,
        start_sequence  : TypeSeq,
        start_time      : String,
        start_time_rel  : u64) -> Self {
        Self {context, locales, source_name, container_id, start_scn, start_sequence, start_time, start_time_rel}
    }
}
