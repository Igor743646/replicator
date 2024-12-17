use crate::{common::{errors::Result, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Ktudb {
    pub xid : TypeXid
}

impl VectorField for Ktudb {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        reader.skip_bytes(8);
        let usn = reader.read_u16()?;
        let slt = reader.read_u16()?;
        let seq = reader.read_u32()?;
        let xid = TypeXid::new(usn, slt, seq);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDB] XID: {}\n", field_num, xid))?;
        }

        Ok(Ktudb { xid })
    }
}
