use crate::{common::{errors::Result, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Ktucm {
    pub xid : TypeXid,
    pub flg : u8,
}

impl VectorField for Ktucm {
    fn parse_from_reader(parser : &mut Parser, vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() == 20, "Size of field {} != 20", reader.data().len());

        let xid_usn = (vec_reader.header.class - 15) / 2;
        let xid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32()?;
        let srt = reader.read_u16()?;
        reader.skip_bytes(2);
        let sta = reader.read_u32()?;
        let flg = reader.read_u8()?;
        reader.skip_bytes(3);
    
        let xid = (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUCM] XID: {}\nFlag: {:08b}\nSRT: {} STA: {}\n", field_num, xid, flg, srt, sta))?;
        }

        Ok(Ktucm { xid, flg })
    }
}
