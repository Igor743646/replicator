use log::warn;

use super::{verify_fields_count, VectorParser};
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}}};

#[derive(Default)]
pub struct OpCode0501 {
    pub xid : TypeXid,

    pub obj : u32,
    pub data_obj : u32,
    pub opc : (u8, u8),
    pub slt : u16,
    pub flg : u16,
}

impl OpCode0501 {
    pub fn ktudb(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 20, "Size of field {} < 20", vector_header.fields_sizes[field_num]);

        reader.skip_bytes(8);
        let usn = reader.read_u16()?;
        let slt = reader.read_u16()?;
        let seq = reader.read_u32()?;
        self.xid = TypeXid::new(usn, slt, seq);
        reader.skip_bytes(4);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDB] XID: {}", field_num, self.xid));
        }

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 20);
        reader.align_up(4);
        Ok(())
    }

    pub fn ktubl(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 24, "Size of field {} < 24", vector_header.fields_sizes[field_num]);

        self.obj = reader.read_u32()?;
        self.data_obj = reader.read_u32()?;
        reader.skip_bytes(4);
        let _undo = reader.read_u32()?;
        self.opc.0 = reader.read_u8()?;
        self.opc.1 = reader.read_u8()?;
        self.slt = reader.read_u8()? as u16;
        reader.skip_bytes(1);
        self.flg = reader.read_u16()?;
        reader.skip_bytes(2);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUBL - {}] OBJ: {} DATAOBJ: {}\nOPC: {}.{} SLT: {}\nFLG: {:016b}\n", 
                    field_num, vector_header.fields_sizes[field_num], self.obj, self.data_obj, self.opc.0, self.opc.1, self.slt, self.flg));

            let tbl = ["NO", "YES"];

            parser.write_dump(format_args!(" MULTI BLOCK UNDO HEAD : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOHEAD != 0) as usize]));
            parser.write_dump(format_args!(" MULTI BLOCK UNDO TAIL : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOTAIL != 0) as usize]));
            parser.write_dump(format_args!(" LAST BUFFER SPLIT     : {:>3}\n", tbl[(self.flg & constants::FLG_LASTBUFFERSPLIT    != 0) as usize]));
            parser.write_dump(format_args!(" BEGIN TRANSACTION     : {:>3}\n", tbl[(self.flg & constants::FLG_BEGIN_TRANS        != 0) as usize]));
            parser.write_dump(format_args!(" USER UNDO DONE        : {:>3}\n", tbl[(self.flg & constants::FLG_USERUNDODDONE      != 0) as usize]));
            parser.write_dump(format_args!(" IS TEMPORARY OBJECT   : {:>3}\n", tbl[(self.flg & constants::FLG_ISTEMPOBJECT       != 0) as usize]));
            parser.write_dump(format_args!(" USER ONLY             : {:>3}\n", tbl[(self.flg & constants::FLG_USERONLY           != 0) as usize]));
            parser.write_dump(format_args!(" TABLESPACE UNDO       : {:>3}\n", tbl[(self.flg & constants::FLG_TABLESPACEUNDO     != 0) as usize]));
            parser.write_dump(format_args!(" MULTI BLOCK UNDO MID  : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOMID  != 0) as usize]));
        }

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 24);
        reader.align_up(4);
        Ok(())
    }

    pub fn ktb_redo(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 8, "Size of field {} < 8", vector_header.fields_sizes[field_num]);
        let prev_cursor = reader.cursor();

        let ktb_op = reader.read_u8()?;
        let flg = reader.read_u8()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTBREDO - {}] OP: {}\n", 
                    field_num, vector_header.fields_sizes[field_num], ktb_op));
        }

        if flg & 0x08 == 0 {
            reader.skip_bytes(2);
        } else {
            reader.skip_bytes(6);
        }

        match ktb_op & 0x0F {
            1 => {
                let usn = reader.read_u16()?;
                let slt = reader.read_u16()?;
                let seq = reader.read_u32()?;
                self.xid = TypeXid::new(usn, slt, seq);

                if parser.can_dump(1) {
                    let uba = reader.read_uba()?;
                    parser.write_dump(format_args!("Op: F XID: {} UBA: {}\n", self.xid, uba));
                } else {
                    reader.skip_bytes(8);
                }
            },
            4 => {
                if !parser.can_dump(1) {
                    reader.skip_bytes(24);
                } else {
                    let usn = reader.read_u16()?;
                    let slt = reader.read_u16()?;
                    let seq = reader.read_u32()?;
                    let itl_xid = TypeXid::new(usn, slt, seq);
                    let uba = reader.read_uba()?;
                    reader.skip_bytes(8);
                    
                    parser.write_dump(format_args!("Op: L ITL XID: {} UBA: {}\n", itl_xid, uba));
                }
            },
            _ => {
                return olr_perr!("Unknown ktb operation: {}. Dump: {}", ktb_op & 0x0F, reader.to_hex_dump());
            },
        }

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - (reader.cursor() - prev_cursor));
        reader.align_up(4);
        Ok(())
    }

    pub fn ktb_opcode(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 16, "Size of field {} < 16", vector_header.fields_sizes[field_num]);


        

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize);
        reader.align_up(4);
        Ok(())
    }

    pub fn opc0b01(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 8, "Size of field {} < 8", vector_header.fields_sizes[field_num]);
        let prev_cursor = reader.cursor();

        

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - (reader.cursor() - prev_cursor));
        reader.align_up(4);
        Ok(())
    }
}

impl VectorParser for OpCode0501 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError> {
        
        let mut result = OpCode0501::default();

        result.ktudb(parser, vector_header, reader, 0)?;

        if vector_header.fields_count < 2 {
            return Ok(());
        }

        result.ktubl(parser, vector_header, reader, 1)?;

        if result.flg & (constants::FLG_MULTIBLOCKUNDOHEAD | constants::FLG_MULTIBLOCKUNDOTAIL | constants::FLG_MULTIBLOCKUNDOMID) != 0 {
            return Ok(());
        }

        if vector_header.fields_count < 3 {
            return Ok(());
        }

        std::unimplemented!();
        match result.opc {
            (11, 1) => {
                result.ktb_redo(parser, vector_header, reader, 2)?;

                if vector_header.fields_count < 4 {
                    return Ok(());
                }

                result.opc0b01(parser, vector_header, reader, 3)?;
            },
            (_, _) => {
                warn!("Unknown 5.1 opc: {}.{}", result.opc.0, result.opc.1);
            },
        }
        
        Ok(())
    }
}
