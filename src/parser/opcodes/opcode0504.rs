use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader}};

#[derive(Default)]
pub struct OpCode0504 {
    pub xid : TypeXid,
    pub flg : u8,
}

impl OpCode0504 {
    pub fn ktucm(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 20, "Size of field {} != 20", reader.data().len());

        let xid_usn = (vector_header.class - 15) / 2;
        let xid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32()?;
        let srt = reader.read_u16()?;
        reader.skip_bytes(2);
        let sta = reader.read_u32()?;
        let flg = reader.read_u8()?;
        reader.skip_bytes(3);
        

        self.xid = (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into();
        self.flg = flg;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUCM] XID: {}\nFlag: {:08b}\nSRT: {} STA: {}\n", field_num, self.xid, self.flg, srt, sta));
        }

        Ok(())
    }

    pub fn ktucf(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 16, "Size of field {} != 16", reader.data().len());

        if parser.can_dump(1) {
            let uba = reader.read_u64()?;
            let ext = reader.read_u16()?;
            let spc = reader.read_u16()?;
            let fbi = reader.read_u8()?;
            reader.skip_bytes(3);
            parser.write_dump(format_args!("\n[Change {}; KTUCF] UBA: {:016X} EXT: {}\nSPC: {} FBI: {}\n", field_num, uba, ext, spc, fbi));
        } else {
            reader.skip_bytes(16);
        }
        
        Ok(())
    }
}

impl VectorParser for OpCode0504 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<(), OLRError> {
        assert!(vector_header.fields_count > 1 && vector_header.fields_count < 5, "Opcode: 5.4 Count of field not in [2; 4]. Dump: {}", reader.map(|x| {x.to_hex_dump()}).collect::<String>());

        let mut result = OpCode0504::default();

        if let Some(mut field_reader) = reader.next() {
            result.ktucm(parser, vector_header, &mut field_reader, 0)?;
        } else {
            return olr_perr!("Expect ktucm field");
        }

        if vector_header.fields_count < 2 {
            return Ok(());
        }
        
        if result.flg & constants::FLAG_KTUCF_OP0504 != 0 {
            if let Some(mut field_reader) = reader.next() {
                result.ktucf(parser, &mut field_reader, 1)?;
            } else {
                return olr_perr!("Expect ktucf field");
            }
        }

        if parser.can_dump(1) && result.flg & constants::FLAG_KTUCF_ROLLBACK != 0 {
            parser.write_dump(format_args!("\nROLLBACK TRANSACTION\n"));
        }

        Ok(())
    }
}
