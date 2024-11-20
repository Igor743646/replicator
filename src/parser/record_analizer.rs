use std::{fs::File, io::Write, path::{Path, PathBuf}};

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
            std::fs::create_dir_all(PathBuf::new().join(self.dump.path.as_str())).unwrap();
            let dump_path = PathBuf::new().join(self.dump.path.as_str()).join(format!("dump-{}.ansi", self.sequence()));
            info!("dsf");
            let mut dump_file = File::create(dump_path).unwrap();
            dump_file.write_fmt(format_args!("\n============\n{}\n============", reader.to_hex_dump())).unwrap();
            info!("dsf");
        }

        Ok(())
    }
}
