use crate::{common::{constants, errors::Result, types::TypeFb}, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;

#[derive(Default)]
pub struct Kdoopcode {
    pub bdba : u32,
    pub op : u8,
    pub flags : u8,
    
    pub fb : Option<TypeFb>,
    pub cc : Option<u8>,
    pub slot : Option<u16>,
    pub size_delt : Option<u16>,
    pub nulls_field : Option<usize>,
    pub nulls_offset : Option<usize>,
    pub slots_offset : Option<usize>,

    pub nrow : Option<u8>,
}

impl Kdoopcode {
    fn kdo_opcode_irp(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<()> {
        assert!(reader.data().len() >= 48, "Size of field {} < 48", reader.data().len());

        result.fb = Some(reader.read_u8()?.into());
        let lb = reader.read_u8()?;
        result.cc = Some(reader.read_u8()?);
        let cki = reader.read_u8()?;

        if parser.can_dump(1) {
            let pos = reader.cursor();
            parser.write_dump(format_args!("FB: {} CC: {} lb: {} cki: {}\n", result.fb.unwrap(), result.cc.unwrap(), lb, cki))?;

            if result.fb.unwrap().is_first() && !result.fb.unwrap().is_head() {
                let hrid1 = reader.read_u32()?;
                let hrid2 = reader.read_u16()?;
                parser.write_dump(format_args!("hrid: {}.{}\n", hrid1, hrid2))?;
            }
            
            if !result.fb.unwrap().is_last() {
                reader.set_cursor(pos + 8)?;
                let nrid_bdba = reader.read_u32()?;
                let nrid_slot = reader.read_u16()?;
                parser.write_dump(format_args!("next bdba: {} next slot: {}\n", nrid_bdba, nrid_slot))?;
            }

            if result.fb.unwrap().is_cluster_key() {
                reader.set_cursor(pos)?;
                let pk = reader.read_u32()?;
                let pk1 = reader.read_u16()?;
                reader.skip_bytes(2);
                let nk = reader.read_u32()?;
                let nk1 = reader.read_u16()?;
                parser.write_dump(format_args!("pk : {} pk1: {} nk: {} nk1: {}\n", pk, pk1, nk, nk1))?;
            }

            reader.set_cursor(pos + 20)?;
        } else {
            reader.skip_bytes(20);
        }

        result.size_delt = Some(reader.read_u16()?);
        result.slot = Some(reader.read_u16()?);
        let tabn = reader.read_u8()?;

        result.nulls_field = Some(field_num);
        result.nulls_offset = Some(reader.cursor());

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {} tabn: {}\n", result.slot.unwrap(), tabn))?;
            parser.write_dump(format_args!("nulls: "))?;

            let mut nulls = reader.read_u8()?;
            for i in 0 .. result.cc.unwrap() {
                let bits = 1u8 << (i & 0b111);

                parser.write_dump(format_args!("{}", (nulls & bits != 0) as u8))?;
                
                if bits == 0b10000000 {
                    nulls = reader.read_u8()?;
                }
            }
            parser.write_dump(format_args!("\n"))?;
            reader.set_cursor(result.nulls_offset.unwrap())?;
        }

        assert!(reader.data().len() >= 45 + ((result.cc.unwrap() as usize + 7) / 8), "Size of field {} < 26 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_drp(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        result.slot = Some(reader.read_u16()?);
        let tabn = reader.read_u8()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {} tabn: {}\n", result.slot.unwrap(), tabn))?;
        }

        Ok(())
    }

    fn kdo_opcode_lkr(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        result.slot = Some(reader.read_u16()?);
        let tabn = reader.read_u8()?;
        let to = reader.read_u8()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {} tabn: {} to: {}\n", result.slot.unwrap(), tabn, to))?;
        }

        Ok(())
    }

    fn kdo_opcode_urp(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 28, "Size of field {} < 28", reader.data().len());

        result.fb = Some(reader.read_u8()?.into());
        let lock = reader.read_u8()?;
        let ckix = reader.read_u8()?;
        let tabn = reader.read_u8()?;
        result.slot = Some(reader.read_u16()?);
        let ncol: u8 = reader.read_u8()?;
        result.cc = Some(reader.read_u8()?);
        let size = reader.read_i16()?;

        result.nulls_offset = Some(reader.cursor());

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", result.fb.unwrap(), result.slot.unwrap(), result.cc.unwrap()))?;
            parser.write_dump(format_args!("lock: {} ckix: {} tabn: {} ncol: {} size: {}\n", lock, ckix, tabn, ncol, size))?;
        }

        assert!(reader.data().len() >= 26 + ((result.cc.unwrap() as usize + 7) / 8), "Size of field {} < 26 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_orp(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 48, "Size of field {} < 48", reader.data().len());

        result.fb = Some(reader.read_u8()?.into());
        reader.skip_bytes(1);
        result.cc = Some(reader.read_u8()?);
        reader.skip_bytes(23);
        result.slot = Some(reader.read_u16()?);
        reader.skip_bytes(1);

        result.nulls_offset = Some(reader.cursor());

        if parser.can_dump(1) {
            parser.write_dump(format_args!("FB: {} SLOT: {} CC: {}\n", result.fb.unwrap(), result.slot.unwrap(), result.cc.unwrap()))?;
        }

        assert!(reader.data().len() >= 45 + ((result.cc.unwrap() as usize + 7) / 8), "Size of field {} < 45 + (cc + 7) / 8", reader.data().len());

        Ok(())
    }

    fn kdo_opcode_cfa(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 32, "Size of field {} < 32", reader.data().len());

        let nrid_bdba = reader.read_u32()?;
        let nrid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        result.slot = Some(reader.read_u16()?);
        let flag = reader.read_u8()?;
        let tabn = reader.read_u8()?;
        let lock = reader.read_u8()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {}\n", result.slot.unwrap()))?;
            parser.write_dump(format_args!("nrid bdba: {} nrid slot: {}\n", nrid_bdba, nrid_slot))?;
            parser.write_dump(format_args!("flag: {} lock: {} tabn: {}\n", flag, lock, tabn))?;
        }

        Ok(())
    }

    fn kdo_opcode_cki(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 20, "Size of field {} < 20", reader.data().len());

        reader.skip_bytes(11);
        result.slot = Some(reader.read_u8()? as u16);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("SLOT: {}\n", result.slot.unwrap()))?;
        }

        Ok(())
    }

    fn kdo_opcode_qm(result : &mut Kdoopcode, parser : &mut Parser, reader : &mut ByteReader) -> Result<()> {
        assert!(reader.data().len() >= 24, "Size of field {} < 24", reader.data().len());

        let tabn = reader.read_u8()?;
        let lock = reader.read_u8()?;
        result.nrow = Some(reader.read_u8()?);
        reader.skip_bytes(1);
        result.slots_offset = Some(reader.cursor());

        if parser.can_dump(1) {
            parser.write_dump(format_args!("NROW: {}\n", result.nrow.unwrap()))?;
            parser.write_dump(format_args!("lock: {} tabn: {}\n", lock, tabn))?;
        }

        Ok(())
    }
}

impl VectorField for Kdoopcode {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self> {
        assert!(reader.data().len() >= 16, "Size of field {} < 16", reader.data().len());

        let mut result = Kdoopcode::default();

        result.bdba = reader.read_u32()?;
        reader.skip_bytes(6);
        result.op = reader.read_u8()?;
        result.flags = reader.read_u8()?;
        reader.skip_bytes(4);

        if parser.can_dump(1) {
            let tbl = ["000", "IUR", "IRP", "DRP", "LKR", "URP", "ORP", "MFC", "CFA", "CKI", "SKL", "QMI", "QMD", "013", "DSC", "015", "LMN", "LLB", "018", "019", "SHK", "021", "CMP", "DCU", "MRK"];
            parser.write_dump(format_args!("\n[Change {}; KTBOPCODE - {}] OP: {}\n", field_num, reader.data().len(), tbl.get((result.op & 0x1F) as usize).or(Some(&"Unknown operation")).unwrap()))?;
            parser.write_dump(format_args!("BDBA: {} OP: {} FLAGS: {}\n", result.bdba, result.op, result.flags))?;
        }

        match result.op & 0x1F {
            constants::OP_IRP => {
                Self::kdo_opcode_irp(&mut result, parser, reader, field_num)?;
            },
            constants::OP_DRP => {
                Self::kdo_opcode_drp(&mut result, parser, reader)?;
            },
            constants::OP_LKR => {
                Self::kdo_opcode_lkr(&mut result, parser, reader)?;
            },
            constants::OP_URP => {
                Self::kdo_opcode_urp(&mut result, parser, reader)?;
            },
            constants::OP_ORP => {
                Self::kdo_opcode_orp(&mut result, parser, reader)?;
            },
            constants::OP_CFA => {
                Self::kdo_opcode_cfa(&mut result, parser, reader)?;
            },
            constants::OP_CKI => {
                Self::kdo_opcode_cki(&mut result, parser, reader)?;
            },
            constants::OP_QMI | constants::OP_QMD => {
                Self::kdo_opcode_qm(&mut result, parser, reader)?;
            },
            _ => ()
        }

        Ok(result)
    }
}
