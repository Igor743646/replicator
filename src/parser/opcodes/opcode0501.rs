use super::{VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::{TypeFb, TypeScn, TypeXid}}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

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
            Some(ref mut field_reader) => self.ktubl(parser, field_reader, 1),
            None => return Ok(()),
        }?;

        if self.flg & constants::FLG_MULTIBLOCKUNDO != 0 || self.reader.eof() {
            return Ok(());
        }

        match self.opc {
            (10, 22) => {
                match self.reader.next() {
                    Some(ref mut field_reader) => self.ktb_redo(parser, field_reader, 2),
                    None => return Ok(()),
                }?;

                self.opc0a16(parser, 3)?;
            },
            (11, 1) => {
                match self.reader.next() {
                    Some(ref mut field_reader) => self.ktb_redo(parser, field_reader, 2),
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
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        reader.skip_bytes(8);
        let usn = reader.read_u16()?;
        let slt = reader.read_u16()?;
        let seq = reader.read_u32()?;
        self.xid = TypeXid::new(usn, slt, seq);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDB] XID: {}", field_num, self.xid))?;
        }

        Ok(())
    }

    fn ktubl(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 24, "Size of field {} < 24", reader.data().len());

        self.obj        = reader.read_u32()?;
        self.data_obj   = reader.read_u32()?;
        reader.skip_bytes(4);
        let _undo  = reader.read_u32()?;
        self.opc.0      = reader.read_u8()?;
        self.opc.1      = reader.read_u8()?;
        self.slt        = reader.read_u8()? as u16;
        reader.skip_bytes(1);
        self.flg        = reader.read_u16()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUBL - {}] OBJ: {} DATAOBJ: {}\nOPC: {}.{} SLT: {}\nFLG: {:016b}\n", 
                    field_num, reader.data().len(), self.obj, self.data_obj, self.opc.0, self.opc.1, self.slt, self.flg))?;

            let tbl = ["NO", "YES"];
            parser.write_dump(format_args!(" MULTI BLOCK UNDO HEAD : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOHEAD != 0) as usize]))?;
            parser.write_dump(format_args!(" MULTI BLOCK UNDO TAIL : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOTAIL != 0) as usize]))?;
            parser.write_dump(format_args!(" LAST BUFFER SPLIT     : {:>3}\n", tbl[(self.flg & constants::FLG_LASTBUFFERSPLIT    != 0) as usize]))?;
            parser.write_dump(format_args!(" BEGIN TRANSACTION     : {:>3}\n", tbl[(self.flg & constants::FLG_BEGIN_TRANS        != 0) as usize]))?;
            parser.write_dump(format_args!(" USER UNDO DONE        : {:>3}\n", tbl[(self.flg & constants::FLG_USERUNDODDONE      != 0) as usize]))?;
            parser.write_dump(format_args!(" IS TEMPORARY OBJECT   : {:>3}\n", tbl[(self.flg & constants::FLG_ISTEMPOBJECT       != 0) as usize]))?;
            parser.write_dump(format_args!(" USER ONLY             : {:>3}\n", tbl[(self.flg & constants::FLG_USERONLY           != 0) as usize]))?;
            parser.write_dump(format_args!(" TABLESPACE UNDO       : {:>3}\n", tbl[(self.flg & constants::FLG_TABLESPACEUNDO     != 0) as usize]))?;
            parser.write_dump(format_args!(" MULTI BLOCK UNDO MID  : {:>3}\n", tbl[(self.flg & constants::FLG_MULTIBLOCKUNDOMID  != 0) as usize]))?;
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
            parser.write_dump(format_args!("\n[Change {}; KTBREDO - {}] ", field_num, reader.data().len()))?;
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
                    parser.write_dump(format_args!("Op: F\nXID: {} UBA: {}\n", self.xid, uba))?;
                }
            },
            constants::KTBOP_C => {
                assert!(reader.data().len() - reader.cursor() >= 8, "Size of field {} < 8", reader.data().len());

                if parser.can_dump(1) {
                    let uba = reader.read_uba()?;
                    parser.write_dump(format_args!("Op: C\nUBA: {}\n", uba))?;
                }
            },
            constants::KTBOP_Z => {
                if parser.can_dump(1) {
                    parser.write_dump(format_args!("Op: Z\n"))?;
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
                    
                    parser.write_dump(format_args!("Op: L\nITL_XID: {} UBA: {}\n", itl_xid, uba))?;
                }
            },
            constants::KTBOP_R => {
                if parser.can_dump(1) {
                    reader.skip_bytes(2);
                    let itc = match reader.read_i16()? {
                        x if x < 0 => 0usize,
                        x => x as usize,
                    };
                    reader.skip_bytes(8);

                    parser.write_dump(format_args!("Op: R\nITC: {}\n", itc))?;

                    assert!(reader.data().len() - reader.cursor() >= 12 + itc * 24, "Size of field {} < 12 * itc * 24", reader.data().len());

                    for i in 0 .. itc {
                        let usn = reader.read_u16()?;
                        let slt = reader.read_u16()?;
                        let seq = reader.read_u32()?;
                        let itc_xid = TypeXid::new(usn, slt, seq);
                        let uba = reader.read_uba()?;
    
                        let mut flags : [u8; 4] = *b"----";
                        let scnfsc : TypeScn;
                        let mut scnfsc_str = "FSC";
                        let mut lck = reader.read_u16()?;
                        
                        if lck & 0x1000 != 0 {flags[3] = b'T';}
                        if lck & 0x2000 != 0 {flags[2] = b'U';}
                        if lck & 0x4000 != 0 {flags[1] = b'B';}
                        if lck & 0x8000 != 0 {
                            flags[0] = b'C';
                            scnfsc_str = "SCN";
                            lck = 0;
                            scnfsc = ((reader.read_u32()? as u64) | ((reader.read_u16()? as u64) << 32)).into();
                        } else {
                            scnfsc = (((reader.read_u16()? as u64) << 32) | (reader.read_u32()? as u64)).into();
                        }
                        lck &= 0x0FFF;

                        parser.write_dump(format_args!("[{}]: ITCXID: {} UBA: {} LCK: {} {}: {}\n", i, itc_xid, uba, lck, scnfsc_str, scnfsc))?;
                    }
                }
            },
            constants::KTBOP_N => {
                if parser.can_dump(1) {
                    parser.write_dump(format_args!("Op: N\n"))?;
                }
            },
            _ => {
                return olr_perr!("Unknown ktb operation: {}. Dump: {}", ktb_op & 0x0F, reader.to_hex_dump());
            },
        }

        if ktb_op & constants::KTBOP_BLOCKCLEANOUT != 0 {
            if parser.can_dump(1) {
                parser.write_dump(format_args!("Block cleanout record\n"))?;
            }
        }

        Ok(())
    }

    fn kdo_opcode_irp(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 48, "Size of field {} < 48", reader.data().len());

        self.fb = reader.read_u8()?.into();
        let lb = reader.read_u8()?;
        self.cc = reader.read_u8()?;
        let cki = reader.read_u8()?;
        reader.skip_bytes(20);
        self.size_delt = reader.read_u16()?;
        self.slot = reader.read_u16()?;
        reader.skip_bytes(1);

        self.nulls_offset = reader.cursor();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", self.fb, self.slot, self.cc))?;
            parser.write_dump(format_args!("lb: {} cki: {} size_delt: {}\n", lb, cki, self.size_delt))?;
            parser.write_dump(format_args!("nulls: "))?;

            let mut nulls = reader.read_u8()?;
            for i in 0 .. self.cc {
                let bits = 1u8 << (i & 0b111);

                parser.write_dump(format_args!("{}", (nulls & bits != 0) as u8))?;
                
                if bits == 0b10000000 {
                    nulls = reader.read_u8()?;
                }
            }
            parser.write_dump(format_args!("\n"))?;
            reader.set_cursor(self.nulls_offset)?;
        }

        assert!(reader.data().len() >= 45 + ((self.cc as usize + 7) / 8), "Size of field {} < 26 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_drp(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        self.slot = reader.read_u16()?;
        reader.skip_bytes(2);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {}\n", self.slot))?;
        }

        Ok(())
    }

    fn kdo_opcode_lkr(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        self.slot = reader.read_u16()?;
        reader.skip_bytes(2);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {}\n", self.slot))?;
        }

        Ok(())
    }

    fn kdo_opcode_urp(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 28, "Size of field {} < 28", reader.data().len());

        self.fb = reader.read_u8()?.into();
        let lock = reader.read_u8()?;
        let ckix = reader.read_u8()?;
        let tabn = reader.read_u8()?;
        self.slot = reader.read_u16()?;
        let ncol: u8 = reader.read_u8()?;
        self.cc = reader.read_u8()?;
        reader.skip_bytes(2);

        self.nulls_offset = reader.cursor();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", self.fb, self.slot, self.cc))?;
            parser.write_dump(format_args!("lock: {} ckix: {} tabn: {} ncol: {}\n", lock, ckix, tabn, ncol))?;
        }

        assert!(reader.data().len() >= 26 + ((self.cc as usize + 7) / 8), "Size of field {} < 26 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_orp(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 48, "Size of field {} < 48", reader.data().len());

        self.fb = reader.read_u8()?.into();
        reader.skip_bytes(1);
        self.cc = reader.read_u8()?;
        reader.skip_bytes(23);
        self.slot = reader.read_u16()?;
        reader.skip_bytes(1);

        self.nulls_offset = reader.cursor();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", self.fb, self.slot, self.cc))?;
        }

        assert!(reader.data().len() >= 45 + ((self.cc as usize + 7) / 8), "Size of field {} < 45 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_qm(&mut self, parser : &mut Parser, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 24, "Size of field {} < 24", reader.data().len());

        let tabn = reader.read_u8()?;
        let lock = reader.read_u8()?;
        self.nrow = reader.read_u8()?;
        reader.skip_bytes(1);
        self.slots_offset = reader.cursor();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("NROW: {}\n", self.nrow))?;
            parser.write_dump(format_args!("lock: {} tabn: {}\n", lock, tabn))?;
        }

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
            let tbl = ["000", "IUR", "IRP", "DRP", "LKR", "URP", "ORP", "MFC", "CFA", "CKI", "SKL", "QMI", "QMD", "013", "DSC", "015", "LMN", "LLB", "018", "019", "SHK", "021", "CMP", "DCU", "MRK"];
            parser.write_dump(format_args!("\n[Change {}; KTBOPCODE - {}] OP: {}\n", field_num, reader.data().len(), tbl.get((self.op & 0x1F) as usize).or(Some(&"Unknown operation")).unwrap()))?;
            parser.write_dump(format_args!("BDBA: {} OP: {} FLAGS: {}\n", self.bdba, self.op, self.flags))?;
        }

        match self.op & 0x1F {
            constants::OP_IRP => {
                self.kdo_opcode_irp(parser, reader)?;
            },
            constants::OP_DRP => {
                self.kdo_opcode_drp(parser, reader)?;
            },
            constants::OP_LKR => {
                self.kdo_opcode_lkr(parser, reader)?;
            },
            constants::OP_URP => {
                self.kdo_opcode_urp(parser, reader)?;
            },
            constants::OP_ORP => {
                self.kdo_opcode_orp(parser, reader)?;
            },
            constants::OP_CFA => std::unimplemented!("{}", self.op & 0x1F),
            constants::OP_CKI => std::unimplemented!("{}", self.op & 0x1F),
            constants::OP_QMI | constants::OP_QMD => {
                self.kdo_opcode_qm(parser, reader)?;
            },
            _ => ()
        }

        Ok(())
    }

    fn supp_log(&mut self, parser : &mut Parser, reader : &mut ByteReader, mut field_num : usize) -> Result<(), OLRError> {
        
        while field_num < self.reader.header.fields_count as usize {
            reader.skip_bytes(self.reader.header.fields_sizes[field_num] as usize);
            reader.align_up(4);
            field_num += 1;
        }

        Ok(())
    }

    fn kdilk(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
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
                .ok_or(olr_perr!("expect ktb opcode field"))?;
    
        self.kdo_opcode(parser, &mut ktb_opcode_reader, field_num)?;

        match self.op & 0x1F {
            constants::OP_IRP | constants::OP_ORP => {
                if self.cc > 0 {
                    if parser.can_dump(1) {
                        let mut nulls: u8 = 0;
                        for mask in (0 .. self.cc).map(|i| 1u8 << (i & 0b111)) {
                            if mask == 1 {
                                nulls = ktb_opcode_reader.read_u8()?;
                            }

                            let column_reader = self.reader.next().unwrap();

                            if nulls & mask == 0 {
                                parser.write_dump(format_args!("Col [{}]: {:02X?}\n", column_reader.data().len(), column_reader.data()))?;
                            } else {
                                assert!(column_reader.data().len() == 0, "Size of field {} != 0", column_reader.data().len());
                                parser.write_dump(format_args!("Col: NULL\n"))?;
                            }
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

                // self.supp_log(parser, vector_header, reader, field_num)?;
            },
            constants::OP_DRP => {
                if self.op & 64 != 0 {
                    std::unimplemented!("{}", self.op & 0x1F);
                }

                // self.supp_log(parser, vector_header, reader, field_num)?;
            },
            constants::OP_URP => {
                if self.flags & 128 != 0 {
                    std::unimplemented!();
                } else {
                    let _ = self.reader.next().unwrap(); // 4 bytes size

                    let mut bits : u8 = 1;
                    let mut nulls: u8 = ktb_opcode_reader.read_u8()?;

                    'col_dumps: for _ in 0 .. self.cc {
                        if nulls & bits == 0 {
                            let column_reader = loop {
                                let column_reader = self.reader.next();

                                if column_reader.is_none() {
                                    break 'col_dumps;
                                }
                                
                                if column_reader.unwrap().data().len() > 0 {
                                    break column_reader.unwrap();
                                }
                            };

                            if parser.can_dump(1) {
                                parser.write_dump(format_args!("Col [{}]: {:02X?}\n", column_reader.data().len(), column_reader.data()))?;
                            }
                        } else {
                            if parser.can_dump(1) {
                                parser.write_dump(format_args!("Col: NULL\n"))?;
                            }
                        }

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

                // self.supp_log(parser, vector_header, reader, field_num + 1)?;
            },
            constants::OP_QMI => {
                let mut sizes_reader = self.reader.next().unwrap();
                let mut data_reader = self.reader.next().unwrap();

                if parser.can_dump(1) {
                    for _ in 0 .. self.nrow {
                        let fb = data_reader.read_u8()?;
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

                        for _ in 0 .. jcc {
                            let mut size: u16 = data_reader.read_u8()? as u16;
                            let is_null: bool = size == 0xFF;

                            if size == 0xFE {
                                size = data_reader.read_u16()?;
                            }

                            if !is_null {
                                parser.write_dump(format_args!("Col [{}]: {:02X?}\n", size, &data_reader.data()[data_reader.cursor() .. data_reader.cursor() + size as usize] ))?;
                                data_reader.skip_bytes(size as usize);
                            }
                        }
                    }
                }

                // self.supp_log(parser, vector_header, reader, field_num)?;
            },
            constants::OP_LKR => {
                // self.supp_log(parser, vector_header, reader, field_num)?;
            },
            constants::OP_SKL | constants::OP_QMD | constants::OP_LMN => {},
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
