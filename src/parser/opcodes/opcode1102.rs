use super::{VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode1102<'a> {
    pub xid : TypeXid,
    pub fb : u8,
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
            Some(mut field_reader) => self.ktb_redo(parser, &mut field_reader, 0),
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
                            parser.write_dump(format_args!("Col [{}]: {:02X?}\n", field_reader.data().len(), field_reader.data()));
                        } else {
                            assert!(field_reader.data().len() == 0, "Size of field {} != 0", field_reader.data().len());
                            parser.write_dump(format_args!("Col: NULL\n"));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn ktb_redo(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 8, "Size of field {} < 8", reader.data().len());

        let ktb_op = reader.read_u8()?;
        let flg = reader.read_u8()?;
        reader.skip_bytes(2);

        if flg & 0x08 != 0 {
            reader.skip_bytes(4);
        }

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTBREDO - {}] OP: {}\n", 
                    field_num, reader.data().len(), ktb_op));
        }

        match ktb_op & 0x0F {
            constants::KTBOP_F => {
                assert!(reader.data().len() - reader.cursor() >= 16, "Size of field {} < 16", reader.data().len());

                let usn = reader.read_u16()?;
                let slt = reader.read_u16()?;
                let seq = reader.read_u32()?;
                self.xid = TypeXid::new(usn, slt, seq);

                if parser.can_dump(1) {
                    let uba = reader.read_uba()?;
                    parser.write_dump(format_args!("Op: F XID: {} UBA: {}\n", self.xid, uba));
                }
            },
            constants::KTBOP_C => {
                assert!(reader.data().len() - reader.cursor() >= 8, "Size of field {} < 8", reader.data().len());

                if parser.can_dump(1) {
                    let uba = reader.read_uba()?;
                    parser.write_dump(format_args!("Op: C UBA: {}\n", uba));
                }
            },
            constants::KTBOP_Z => {
                if parser.can_dump(1) {
                    parser.write_dump(format_args!("Op: Z\n"));
                }
            },
            constants::KTBOP_L => {
                assert!(reader.data().len() - reader.cursor() >= 24, "Size of field {} < 24", reader.data().len());

                if parser.can_dump(1) {
                    let usn = reader.read_u16()?;
                    let slt = reader.read_u16()?;
                    let seq = reader.read_u32()?;
                    let itl_xid = TypeXid::new(usn, slt, seq);
                    let uba = reader.read_uba()?;
                    reader.skip_bytes(8);
                    
                    parser.write_dump(format_args!("Op: L ITL_XID: {} UBA: {}\n", itl_xid, uba));
                }
            },
            _ => {
                return olr_perr!("Unknown ktb operation: {}. Dump: {}", ktb_op & 0x0F, reader.to_hex_dump());
            },
        }

        Ok(())
    }

    fn kdo_opcode_irp(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 48, "Size of field {} < 48", reader.data().len());

        self.fb = reader.read_u8()?;
        let lb = reader.read_u8()?;
        self.cc = reader.read_u8()?;
        let cki = reader.read_u8()?;
        reader.skip_bytes(20);
        self.size_delt = reader.read_u16()?;
        self.slot = reader.read_u16()?;
        reader.skip_bytes(1);

        self.nulls_field = field_num;
        self.nulls_offset = reader.cursor();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", self.fb, self.slot, self.cc));
            parser.write_dump(format_args!("lb: {} cki: {} size_delt: {}\n", lb, cki, self.size_delt));
            parser.write_dump(format_args!("nulls: "));

            let mut nulls = reader.read_u8()?;
            for i in 0 .. self.cc {
                let bits = 1u8 << (i & 0b111);

                parser.write_dump(format_args!("{}", (nulls & bits != 0) as u8));
                
                if bits == 0b10000000 {
                    nulls = reader.read_u8()?;
                }
            }
            parser.write_dump(format_args!("\n"));
            reader.set_cursor(self.nulls_offset)?;
        }

        assert!(reader.data().len() >= 45 + ((self.cc as usize + 7) / 8), "Size of field {} < 26 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 16, "Size of field {} < 16", reader.data().len());

        self.bdba = reader.read_u32()?;
        reader.skip_bytes(6);
        self.op = reader.read_u8()?;
        self.flags = reader.read_u8()?;
        reader.skip_bytes(4);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTBOPCODE - {}] OP: IRP\n", field_num, reader.data().len()));
            parser.write_dump(format_args!("BDBA: {} OP: {} FLAGS: {}\n", self.bdba, self.op, self.flags));
        }

        assert!(self.op & 0x1F == constants::OP_IRP, "Operation is not IRP");
        self.kdo_opcode_irp(parser, reader, field_num)?;

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
