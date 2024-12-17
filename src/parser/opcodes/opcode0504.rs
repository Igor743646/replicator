use super::{fields::{ktucf::Ktucf, ktucm::Ktucm, VectorField}, VectorData, VectorParser};
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
            parser.write_dump(format_args!("\nROLLBACK TRANSACTION\n"))?;
        }

        Ok(())
    }

    fn ktucm(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let ktucm = Ktucm::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.xid = ktucm.xid;
        self.flg = ktucm.flg;
        Ok(())
    }

    fn ktucf(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        Ktucf::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        Ok(())
    }
}

impl<'a> VectorParser<'a> for OpCode0504<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorData<'a>, OLRError> {
        Ok(
            VectorData::OpCode0504(
                OpCode0504::new(parser, reader)?
            )
        )
    }
}
