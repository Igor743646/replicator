use std::{fs::File, io::Write, path::PathBuf};

use log::info;

use crate::common::errors::OLRError;

use super::{byte_reader::ByteReader, parser_impl::Parser, records_manager::Record};


pub trait RecordAnalizer {
    fn analize_record(&mut self, record_ptr : *mut Record) -> Result<(), OLRError>;
}

impl RecordAnalizer for Parser {
    fn analize_record(&mut self, record_ptr : *mut Record) -> Result<(), OLRError> {
        
        let record = unsafe { record_ptr.as_ref().unwrap() };

        let data = unsafe {
            std::ptr::slice_from_raw_parts(
                (record_ptr as *mut u8).add(size_of::<Record>()), 
                record.size as usize
            ).as_ref().unwrap()
        };

        let mut reader = ByteReader::from_bytes(data);
        let sz = reader.read_u32().unwrap();
        
        assert_eq!(sz, record.size);

        if record.block == 2 {
            self.write_dump(format_args!("\n============\n{}\n============", reader.to_hex_dump()));
        }

        Ok(())
    }
}
