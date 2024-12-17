use crate::{common::errors::Result, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;

pub struct Pdb {
}

impl VectorField for Pdb {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() == 4, "Size of field {} != 4", reader.data().len());

        if parser.can_dump(1) {
            let pdb_id = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}; PDB] PDB id: {}\n", field_num, pdb_id))?;
        }
        
        Ok(Pdb {})
    }
}
