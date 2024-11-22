use super::VectorParser;
use crate::{common::{errors::OLRError, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}}};

pub struct OpCode0502 {
    pub xid : TypeXid,
    pub flg : u16,
}

impl VectorParser for OpCode0502 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(vector_header.fields_count <= 3, "OpCode: 5.2 Fields Count: {}", vector_header.fields_count);
        assert!(vector_header.fields_sizes[0] >= 32, "OpCode: 5.2 Size of first field {} < 32", vector_header.fields_sizes[0]);
        reader.align_up(4);

        let xid_usn = (vector_header.class - 15) / 2;
        let xid_slot = reader.read_u16().unwrap();
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32().unwrap();
        reader.skip_bytes(8);
        let flg = reader.read_u16().unwrap();
        reader.skip_bytes(14);

        let result = OpCode0502 {
            xid : (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into(),
            flg
        };

        parser.write_dump(format_args!("\n[Change {}] OpCode: 5.2 XID: {} Flag: {:016b}\n", 1, result.xid, result.flg));

        for i in vector_header.fields_sizes.iter().skip(1) {
            reader.align_up(4);
            reader.skip_bytes(*i as usize);
        }
        
        reader.align_up(4);
        Ok(())
    }
}
