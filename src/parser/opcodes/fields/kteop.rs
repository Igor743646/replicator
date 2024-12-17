use crate::{common::errors::Result, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Kteop {
}

impl VectorField for Kteop {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() == 36, "Size of field {} != 36", reader.data().len());

        if parser.can_dump(1) {
            reader.skip_bytes(4);
            let ext = reader.read_u32()?;
            reader.skip_bytes(4);
            let ext_size = reader.read_u32()?;
            let highwater = reader.read_u32()?;
            reader.skip_bytes(4);
            let offset = reader.read_u32()?;

            parser.write_dump(format_args!("\n[Change {}; KTEOP] ext: {} ext size: {} HW: {} offset: {}\n", field_num, ext, ext_size, highwater, offset))?;
        }

        Ok(Kteop {})
    }
}
