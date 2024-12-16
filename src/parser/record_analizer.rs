use crate::common::errors::OLRError;
use super::records_manager::Record;

pub trait RecordAnalizer {
    fn analize_record(&mut self, record_ptr : &Record) -> Result<(), OLRError>;
}


