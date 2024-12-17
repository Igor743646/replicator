use super::{fields::{kteop::Kteop, ktudh::Ktudh, pdb::Pdb, VectorField}, VectorData, VectorParser};
use crate::{common::{constants, errors::Result, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode0502<'a> {
    pub xid : TypeXid,
    pub flg : u16,

    reader : VectorReader<'a>,
}

impl<'a> OpCode0502<'a> {
    pub fn new(parser : &mut Parser, reader : VectorReader<'a>) -> Result<Self> {
        let mut res = Self {
            xid : Default::default(),
            flg : Default::default(),
            reader,
        };
        res.init(parser)?;
        Ok(res)
    }

    fn init(&mut self, parser : &mut Parser) -> Result<()> {
        if self.reader.header.fields_count > 3 {
            return olr_perr!("Opcode: 5.2 Count of field > 3. Dump: {}", self.reader.by_ref().map(|x| {x.to_hex_dump()}).collect::<String>());
        }

        match self.reader.next() {
            Some(mut field_reader) => self.ktudh(parser, &mut field_reader, 0),
            None => olr_perr!("Expect ktudh field")
        }?;

        if parser.version().unwrap() >= constants::REDO_VERSION_12_1 && parser.can_dump(1) {
            if let Some(mut field_reader) = self.reader.next() {
                if field_reader.data().len() == 4 {
                    self.pdb(parser, &mut field_reader, 1)?;
                } else {
                    self.kteop(parser, &mut field_reader, 1)?;

                    if let Some(mut field_reader) = self.reader.next() {
                        self.pdb(parser, &mut field_reader, 2)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn ktudh(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<()> {
        let ktudh = Ktudh::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.xid = ktudh.xid;
        self.flg = ktudh.flg;
        Ok(())
    }

    fn pdb(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<()> {
        Pdb::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        Ok(())
    }

    fn kteop(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<()> {
        Kteop::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        Ok(())
    }
}

impl<'a> VectorParser<'a> for OpCode0502<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorData<'a>> {
        Ok(
            VectorData::OpCode0502(
                OpCode0502::new(parser, reader)?
            )
        )
    }
}
