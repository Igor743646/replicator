use super::{VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode0504<'a> {
    pub xid : TypeXid,
    pub flg : u8,

    reader : VectorReader<'a>,
}

impl<'a> OpCode0504<'a> {
    pub fn new(parser : &mut Parser, reader : VectorReader<'a>) -> Result<Self, OLRError> {
        let mut res = Self {
            xid : Default::default(),
            flg : Default::default(),
            reader,
        };
        res.init(parser)?;
        Ok(res)
    }

    fn init(&mut self, parser : &mut Parser) -> Result<(), OLRError> {
        if !(self.reader.header.fields_count > 0 && self.reader.header.fields_count < 5) {
            return olr_perr!("Opcode: 5.4 Count of field not in [1; 4]. Dump: {}", self.reader.by_ref().map(|x| {x.to_hex_dump()}).collect::<String>());
        }
        
        match self.reader.next() {
            Some(mut field_reader) => self.ktucm(parser, &mut field_reader, 0),
            None => olr_perr!("Expect ktucm field"),
        }?;

        if self.reader.header.fields_count < 2 {
            return Ok(());
        }
        
        if self.flg & constants::FLAG_KTUCF_OP0504 != 0 {
            match self.reader.next() {
                Some(mut field_reader) => self.ktucf(parser, &mut field_reader, 1),
                None => olr_perr!("Expect ktucf field"),
            }?;
        }

        if parser.can_dump(1) && (self.flg & constants::FLAG_KTUCF_ROLLBACK != 0) {
            parser.write_dump(format_args!("\nROLLBACK TRANSACTION\n"));
        }

        Ok(())
    }

    fn ktucm(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 20, "Size of field {} != 20", reader.data().len());

        let xid_usn = (self.reader.header.class - 15) / 2;
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

    fn ktucf(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 16, "Size of field {} != 16", reader.data().len());

        if parser.can_dump(1) {
            let uba = reader.read_u64()?;
            let ext = reader.read_u16()?;
            let spc = reader.read_u16()?;
            let fbi = reader.read_u8()?;
            parser.write_dump(format_args!("\n[Change {}; KTUCF] UBA: {:016X} EXT: {}\nSPC: {} FBI: {}\n", field_num, uba, ext, spc, fbi));
        }
        
        Ok(())
    }
}

impl<'a> VectorParser<'a> for OpCode0504<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorInfo<'a>, OLRError> {
        Ok(
            VectorInfo::OpCode0504(
                OpCode0504::new(parser, reader)?
            )
        )
    }
}
