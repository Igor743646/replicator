use crate::{common::errors::OLRError, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Ktucf {
}

impl VectorField for Ktucf {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self, OLRError> {
        assert!(reader.data().len() == 16, "Size of field {} != 16", reader.data().len());

        if parser.can_dump(1) {
            let uba = reader.read_u64()?;
            let ext = reader.read_u16()?;
            let spc = reader.read_u16()?;
            let fbi = reader.read_u8()?;
            parser.write_dump(format_args!("\n[Change {}; KTUCF] UBA: {:016X} EXT: {}\nSPC: {} FBI: {}\n", field_num, uba, ext, spc, fbi))?;
        }
        
        Ok(Ktucf {})
    }
}
