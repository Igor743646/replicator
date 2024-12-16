use super::{fields::{kdoopcode::Kdoopcode, ktbredo::Ktbredo, VectorField}, VectorInfo, VectorParser};
use crate::{common::{errors::OLRError, types::{TypeFb, TypeXid}}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode1102<'a> {
    pub xid : TypeXid,
    pub fb : TypeFb,
    pub cc : u8,
    pub size_delt : u16,
    pub slot : u16,

    pub nulls_field : usize,
    pub nulls_offset : usize,

    pub data_field : usize,

    pub bdba : u32,
    pub op : u8,
    pub flags : u8,
    
    reader : VectorReader<'a>,
}

impl<'a> OpCode1102<'a> {
    pub fn new(parser : &mut Parser, reader : VectorReader<'a>) -> Result<Self, OLRError> {
        let mut res = Self {
            xid : Default::default(),
            fb : Default::default(),
            cc : Default::default(),
            size_delt : Default::default(),
            slot : Default::default(),
            nulls_field : Default::default(),
            nulls_offset : Default::default(),
            data_field : Default::default(),
            bdba : Default::default(),
            op : Default::default(),
            flags : Default::default(),
            reader,
        };
        res.init(parser)?;
        Ok(res)
    }

    fn init(&mut self, parser : &mut Parser) -> Result<(), OLRError> {

        match self.reader.next() {
            Some(mut field_reader) => self.ktbredo(parser, &mut field_reader, 0),
            None => olr_perr!("Expect ktb_redo field")
        }?;
        
        let mut ktb_opcode_reader = if let Some(mut field_reader) = self.reader.next() {
            self.kdo_opcode(parser, &mut field_reader, 1)?;
            field_reader
        } else {
            return Ok(());
        };

        self.data_field = 2;
        if let Some(mut field_reader) = self.reader.next() {
            if field_reader.data().len() == self.size_delt as usize && self.cc != 1 {
                std::unimplemented!("Compressed data");
            } else {
                let mut nulls: u8 = 0;
                ktb_opcode_reader.set_cursor(self.nulls_offset)?;
                for i in 0 .. self.cc {
                    let mask = 1u8 << (i & 0b111);
                    if mask == 1 {
                        nulls = ktb_opcode_reader.read_u8()?;
                    }
                    
                    if i > 0 {
                        field_reader = self.reader.next().unwrap();
                    }

                    if parser.can_dump(1) {
                        if nulls & mask == 0 {
                            parser.write_dump(format_args!("Col [{}]: {:02X?}\n", field_reader.data().len(), field_reader.data()))?;
                        } else {
                            assert!(field_reader.data().len() == 0, "Size of field {} != 0", field_reader.data().len());
                            parser.write_dump(format_args!("Col: NULL\n"))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn ktbredo(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let ktbredo = Ktbredo::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        if let Some(xid) = ktbredo.xid { self.xid = xid; } 
        Ok(())
    }

    fn kdo_opcode(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let kdoopcode = Kdoopcode::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.bdba = kdoopcode.bdba;
        self.op = kdoopcode.op;
        self.flags = kdoopcode.flags;

        if let Some(fb) = kdoopcode.fb { self.fb = fb; }
        if let Some(cc) = kdoopcode.cc { self.cc = cc; }
        if let Some(slot) = kdoopcode.slot { self.slot = slot; }
        if let Some(size_delt) = kdoopcode.size_delt { self.size_delt = size_delt; }
        if let Some(nulls_field) = kdoopcode.nulls_field { self.nulls_field = nulls_field; }
        if let Some(nulls_offset) = kdoopcode.nulls_offset { self.nulls_offset = nulls_offset; }

        Ok(())
    }

    pub fn get_nulls_field(&self) -> ByteReader {
        let mut res = self.reader.get_field_nth(self.nulls_field);
        res.set_cursor(self.nulls_offset).unwrap();
        res
    }

    pub fn get_data_field(&self, n : usize) -> ByteReader {
        self.reader.get_field_nth(self.data_field + n)
    }
}

impl<'a> VectorParser<'a> for OpCode1102<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorInfo<'a>, OLRError> {
        Ok(
            VectorInfo::OpCode1102(
                OpCode1102::new(parser, reader)?
            )
        )
    }
}
