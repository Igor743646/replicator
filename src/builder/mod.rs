use std::sync::{Arc, RwLock};
use log::info;

use formats::BuilderFormats;
use queue::BuilderQueue;

use crate::{common::errors::OLRError, ctx::Ctx, locales::Locales, metadata::Metadata};

pub mod formats;
pub mod queue;

pub struct JsonBuilder<'a> {
    context_ptr : Arc<RwLock<Ctx>>, 
    locales_ptr : Arc<RwLock<Locales>>, 
    metadata_ptr : Arc<RwLock<Metadata>>, 
    formats : BuilderFormats,
    queue : BuilderQueue<'a>,
}

impl<'a> JsonBuilder<'a> {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>, locales_ptr : Arc<RwLock<Locales>>, metadata_ptr : Arc<RwLock<Metadata>>, 
                db_format : u8, attributes_format : u8, interval_dts_format : u8, interval_ytm_format : u8, message_format : u8, 
                rid_format : u8, xid_format : u8, timestamp_format : u8, timestamp_tz_format : u8, timestamp_all : u8, char_format : u8,
                scn_format : u8, scn_all : u8, unknown_format : u8, schema_format : u8, column_format : u8, unknown_type : u8) -> Result<Self, OLRError> {
        info!("Initialize JsonBuilder");
        Ok(Self {
            context_ptr : context_ptr.clone(), locales_ptr, metadata_ptr, 
            formats : BuilderFormats {
                db_format, 
                attributes_format, 
                interval_dts_format, 
                interval_ytm_format, 
                message_format, 
                rid_format, 
                xid_format, 
                timestamp_format, 
                timestamp_tz_format, 
                timestamp_all, 
                char_format, 
                scn_format, 
                scn_all, 
                unknown_format, 
                schema_format, 
                column_format, 
                unknown_type,
            },
            queue : BuilderQueue::new(context_ptr)?,
        })
    }
}
