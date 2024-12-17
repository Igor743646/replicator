use crate::{common::{constants, errors::Result, types::{TypeScn, TypeXid}}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Ktbredo {
    pub xid : Option<TypeXid>
}

impl VectorField for Ktbredo {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() >= 8, "Size of field {} < 8", reader.data().len());

        let mut result = Ktbredo { xid : Default::default() };

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
                result.xid = Some(TypeXid::new(usn, slt, seq));

                if parser.can_dump(1) {
                    let uba = reader.read_uba()?;
                    parser.write_dump(format_args!("Op: F\nXID: {} UBA: {}\n", result.xid.unwrap(), uba))?;
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

        Ok(result)
    }
}
