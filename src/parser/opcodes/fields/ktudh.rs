use crate::{common::{errors::OLRError, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;

pub struct Ktudh {
    pub xid : TypeXid,
    pub flg : u16,
}

impl VectorField for Ktudh {
    fn parse_from_reader(parser : &mut Parser, vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self, OLRError> {
        assert!(reader.data().len() == 32, "Size of field {} != 32", reader.data().len());

        let xid_usn = (vec_reader.header.class - 15) / 2;
        let xid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32()?;
        reader.skip_bytes(8);
        let flg = reader.read_u16()?;
        reader.skip_bytes(14);

        let xid = (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDH] XID: {}\nFlag: {:016b}\n", field_num, xid, flg))?;
        }

        Ok(Self { xid, flg })
    }
}
