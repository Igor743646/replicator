use super::{fields::{kdoopcode::Kdoopcode, ktbredo::Ktbredo, ktub::Ktub, ktudb::Ktudb, VectorField}, VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::{TypeFb, TypeXid}}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode0501<'a> {
    pub xid : TypeXid,

    pub obj : u32,
    pub data_obj : u32,
    pub opc : (u8, u8),
    pub slt : u16,
    pub flg : u16,

    pub bdba : u32,
    pub op : u8,
    pub flags : u8,
    pub slot : u16,

    pub fb : TypeFb,
    pub cc : u8,
    pub size_delt : u16,
    pub nulls_offset : usize,
    pub slots_offset : usize,

    pub nrow : u8,
    
    reader : VectorReader<'a>,
}

impl<'a> OpCode0501<'a> {
    pub fn new(parser : &mut Parser, reader : VectorReader<'a>) -> Result<Self, OLRError> {
        let mut res = Self {
            xid : Default::default(),
            obj : Default::default(),
            data_obj : Default::default(),
            opc : Default::default(),
            slt : Default::default(),
            flg : Default::default(),
            bdba : Default::default(),
            op : Default::default(),
            flags : Default::default(),
            slot : Default::default(),
            fb : Default::default(),
            cc : Default::default(),
            size_delt : Default::default(),
            nulls_offset : Default::default(),
            slots_offset : Default::default(),
            nrow : Default::default(),
            reader,
        };
        res.init(parser)?;
        Ok(res)
    }

    fn init(&mut self, parser : &mut Parser) -> Result<(), OLRError> {
        
        match self.reader.next() {
            Some(ref mut field_reader) => self.ktudb(parser, field_reader, 0),
            None => olr_perr!("Expect ktudb field")
        }?;

        match self.reader.next() {
            Some(ref mut field_reader) => self.ktub(parser, field_reader, 1),
            None => return Ok(()),
        }?;

        if self.flg & constants::FLG_MULTIBLOCKUNDO != 0 || self.reader.eof() {
            return Ok(());
        }

        match self.opc {
            (10, 22) => {
                match self.reader.next() {
                    Some(ref mut field_reader) => self.ktbredo(parser, field_reader, 2),
                    None => return Ok(()),
                }?;

                self.opc0a16(parser, 3)?;
            },
            (11, 1) => {
                match self.reader.next() {
                    Some(ref mut field_reader) => self.ktbredo(parser, field_reader, 2),
                    None => return Ok(()),
                }?;

                self.opc0b01(parser, 3)?;
            },
            (26, 1) => std::unimplemented!(),
            (14, 8) => () /* kteoputrn field */,
            (_, _) => (),
        }

        Ok(())
    }

    fn ktudb(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let ktudb = Ktudb::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.xid = ktudb.xid;
        Ok(())
    }

    fn ktub(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let ktub = Ktub::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.obj = ktub.obj;
        self.data_obj = ktub.data_obj;
        self.opc = ktub.opc;
        self.slt = ktub.slt;
        self.flg = ktub.flg;
        Ok(())
    }

    fn ktbredo(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let ktbredo = Ktbredo::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        if let Some(xid) = ktbredo.xid {
            self.xid = xid;
        } 
        Ok(())
    }

    fn kdoopcode(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        let kdoopcode = Kdoopcode::parse_from_reader(parser, &mut self.reader, reader, field_num)?;
        self.bdba = kdoopcode.bdba;
        self.op = kdoopcode.op;
        self.flags = kdoopcode.flags;

        if let Some(fb) = kdoopcode.fb { self.fb = fb; }
        if let Some(cc) = kdoopcode.cc { self.cc = cc; }
        if let Some(slot) = kdoopcode.slot { self.slot = slot; }
        if let Some(size_delt) = kdoopcode.size_delt { self.size_delt = size_delt; }
        if let Some(nulls_offset) = kdoopcode.nulls_offset { self.nulls_offset = nulls_offset; }
        if let Some(slots_offset) = kdoopcode.slots_offset { self.slots_offset = slots_offset; }
        if let Some(nrow) = kdoopcode.nrow { self.nrow = nrow; }

        Ok(())
    }

    fn supp_log(&mut self, parser : &mut Parser, mut field_num : usize) -> Result<(), OLRError> {
        field_num = field_num + self.reader.skip_empty() + 1;

        let supplog_cc;

        match self.reader.next() {
            None => return Ok(()),
            Some(mut field_reader) => {
                assert!(field_reader.data().len() >= 20, "Size of field {} < 20", field_reader.data().len());

                let supplog_type: u8 = field_reader.read_u8()?;
                let supplog_fb: TypeFb = field_reader.read_u8()?.into();
                supplog_cc = field_reader.read_u16()?;
                field_reader.skip_bytes(2);
                let supplog_before = field_reader.read_u16()?;
                let supplog_after = field_reader.read_u16()?;

                if parser.can_dump(2) {
                    parser.write_dump(format_args!("\n[Change {}; SuppLog - {}] Type: {} FB : {} CC: {}\nBefore: {} After: {}\n", 
                        field_num, field_reader.data().len(), supplog_type, supplog_fb, supplog_cc, supplog_before, supplog_after))?;
                }

                if field_reader.data().len() >= 26 {
                    field_reader.skip_bytes(10);
                    let supplog_bdba = field_reader.read_u32()?;
                    let supplog_slot = field_reader.read_u16()?;
                    if parser.can_dump(2) {
                        parser.write_dump(format_args!("Bdba: {} Slot: {}\n", supplog_bdba, supplog_slot))?;
                    }
                }
            }
        }

        field_num += 1;
        let mut supplog_numbers = match self.reader.next() {
            None => return Ok(()),
            Some(mut field_reader) => {
                if parser.can_dump(2) {
                    parser.write_dump(format_args!("\n[Change {}; SuppLog - {}] Column numbers: {}\n", field_num, field_reader.data().len(),
                        (0 .. supplog_cc).map(|_| format!("{} ", field_reader.read_u16().unwrap()) ).collect::<String>() ))?;
                }
                field_reader.reset_cursor();
                field_reader
            }
        };

        field_num += 1;
        let mut supplog_lengths = match self.reader.next() {
            None => return Ok(()),
            Some(mut field_reader) => {
                if parser.can_dump(2) {
                    parser.write_dump(format_args!("\n[Change {}; SuppLog - {}] Column lengths: {}\n", field_num, field_reader.data().len(),
                        (0 .. supplog_cc).map(|_| format!("{} ", field_reader.read_u16().unwrap()) ).collect::<String>() ))?;
                }
                field_reader.reset_cursor();
                field_reader
            }
        };

        if parser.can_dump(2) {
            for _ in 0 .. supplog_cc {
                let field_reader = self.reader.next().unwrap();

                let col_num = supplog_numbers.read_u16()? as usize;
                let col_len = supplog_lengths.read_u16()? as usize;
                parser.dump_column(&field_reader.data(), col_num, col_len % 65535, col_len == 65535)?;
            }
        }

        Ok(())
    }

    fn kdilk(&mut self, parser : &mut Parser, reader : &mut ByteReader, _field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        // if parser.can_dump(1) {
        //     parser.write_dump(format_args!("{}", reader.to_hex_dump()))?;
        // }

        Ok(())
    }

    fn opc0a16(&mut self, parser : &mut Parser, field_num : usize) -> Result<(), OLRError> {
        if let Some(mut field_reader) = self.reader.next()  {
            self.kdilk(parser, &mut field_reader, field_num)?;
        } else {
            return olr_perr!("expect kdilk opcode field");
        }

        Ok(())
    }

    fn opc0b01(&mut self, parser : &mut Parser, field_num : usize) -> Result<(), OLRError> {

        let mut ktb_opcode_reader = self.reader.next()
                .ok_or(olr_perr!("Expected ktb opcode field"))?;
    
        self.kdoopcode(parser, &mut ktb_opcode_reader, field_num)?;

        match self.op & 0x1F {
            constants::OP_IRP | constants::OP_ORP => {
                if self.cc > 0 {
                    if parser.can_dump(1) {
                        let mut nulls: u8 = 0;
                        for (i, mask) in (0 .. self.cc).map(|i| (i, 1u8 << (i & 0b111))) {
                            if mask == 1 {
                                nulls = ktb_opcode_reader.read_u8()?;
                            }

                            let column_reader = self.reader.next().unwrap();

                            parser.dump_column(&column_reader.data(), i as usize, column_reader.data().len(), nulls & mask != 0)?;
                        }
                    } else {
                        for _ in 0 .. self.cc {
                            let _ = self.reader.next().unwrap();
                        }
                    }
                }

                if self.op & 64 != 0 {
                    std::unimplemented!("{}", self.op & 0x1F);
                }

                self.supp_log(parser, field_num)?;
            },
            constants::OP_DRP => {
                if self.op & 64 != 0 {
                    std::unimplemented!("{}", self.op & 0x1F);
                }

                self.supp_log(parser, field_num)?;
            },
            constants::OP_URP => {
                if self.flags & 128 != 0 {
                    std::unimplemented!();
                } else {
                    let _ = self.reader.next().unwrap(); // 4 bytes size

                    let mut bits : u8 = 1;
                    let mut nulls: u8 = ktb_opcode_reader.read_u8()?;

                    for i in 0 .. self.cc {
                        let column_reader = self.reader.next().unwrap();

                        parser.dump_column(&column_reader.data(), i as usize, column_reader.data().len(), nulls & bits != 0)?;

                        bits <<= 1;
                        if bits == 0 {
                            bits = 1;
                            nulls = ktb_opcode_reader.read_u8()?;
                        }
                    }
                }

                if self.op & 64 != 0 {
                    std::unimplemented!();
                }

                self.supp_log(parser, field_num)?;
            },
            constants::OP_QMI => {
                let mut sizes_reader = self.reader.next().unwrap();
                let mut data_reader = self.reader.next().unwrap();

                if parser.can_dump(1) {
                    for _ in 0 .. self.nrow {
                        let fb: TypeFb = data_reader.read_u8()?.into();
                        let lb = data_reader.read_u8()?;
                        let jcc = data_reader.read_u8()?;
                        let tl = sizes_reader.read_u16()?;

                        parser.write_dump(format_args!("FB: {} LB: {} TL: {} JCC: {}\n", fb, lb, tl, jcc))?;

                        if self.op & 64 != 0 {
                            if parser.version().unwrap() < constants::REDO_VERSION_12_2 {
                                data_reader.skip_bytes(6);
                            } else {
                                data_reader.skip_bytes(8);
                            }
                        }

                        for j in 0 .. jcc {
                            let mut size: u16 = data_reader.read_u8()? as u16;
                            let is_null: bool = size == 0xFF;

                            if size == 0xFE {
                                size = data_reader.read_u16()?;
                            }

                            parser.dump_column(&data_reader.data()[data_reader.cursor() ..], j as usize, size as usize, is_null)?;
                            if !is_null {
                                data_reader.skip_bytes(size as usize);
                            }
                        }
                    }
                }
            },
            constants::OP_LKR | constants::OP_LMN | constants::OP_CFA => {
                self.supp_log(parser, field_num)?;
            },
            constants::OP_SKL | constants::OP_QMD => {},
            _ => std::unimplemented!("{}", self.op & 0x1F),
        }

        Ok(())
    }
}

impl<'a> VectorParser<'a> for OpCode0501<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorInfo<'a>, OLRError> {
        Ok(
            VectorInfo::OpCode0501(
                OpCode0501::new(parser, reader)?
            )
        )
    }
}
