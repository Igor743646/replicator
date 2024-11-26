use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}}};

#[derive(Default)]
pub struct OpCode0504 {
    pub xid : TypeXid,
    pub flg : u8,
}

impl OpCode0504 {
    pub fn ktucm(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 20, "Size of field {} < 20", vector_header.fields_sizes[field_num]);

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

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 20);
        reader.align_up(4);
        Ok(())
    }

    pub fn ktucf(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 16, "Size of field {} < 16", vector_header.fields_sizes[field_num]);

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
        
        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 16);
        reader.align_up(4);
        Ok(())
    }

    pub fn unknown_field(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 4, "Size of field {} < 4", vector_header.fields_sizes[field_num]);

        if parser.can_dump(1) {
            let unknown_value = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}] Unknown value: {}\n", field_num, unknown_value));
        } else {
            reader.skip_bytes(4);
        }

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 4);
        reader.align_up(4);
        Ok(())
    }
}

impl VectorParser for OpCode0504 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError> {
        if vector_header.fields_count > 3 {
            return olr_perr!("Count of fields ({}) > 4. Dump: {}", vector_header.fields_count, reader.to_hex_dump());
        }

        let mut result = OpCode0504::default();

        result.ktucm(parser, vector_header, reader, 0)?;

        if vector_header.fields_count < 2 {
            return Ok(());
        }
        
        if result.flg & constants::FLAG_KTUCF_OP0504 != 0 {
            result.ktucf(parser, vector_header, reader, 1)?;
        } else {
            result.unknown_field(parser, vector_header, reader, 1)?;
        }

        if parser.can_dump(1) && result.flg & constants::FLAG_KTUCF_ROLLBACK != 0 {
            parser.write_dump(format_args!("\nROLLBACK TRANSACTION\n"));
        }

        if vector_header.fields_count < 3 {
            return Ok(());
        }

        result.unknown_field(parser, vector_header, reader, 2)?;

        Ok(())
    }
}
