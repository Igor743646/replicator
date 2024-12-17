use crate::common::errors::Result;
use super::records_manager::Record;

pub trait RecordAnalizer {
    fn analize_record(&mut self, record_ptr : &Record) -> Result<()>;
}


