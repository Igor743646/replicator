use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader}};

#[derive(Default)]
pub struct OpCode0502 {
    pub xid : TypeXid,
    pub flg : u16,
}

impl OpCode0502 {
    pub fn ktudh(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 32, "Size of field {} < 32", reader.data().len());

        let xid_usn = (vector_header.class - 15) / 2;
        let xid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32()?;
        reader.skip_bytes(8);
        let flg = reader.read_u16()?;
        reader.skip_bytes(14);

        self.xid = (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into();
        self.flg = flg;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDH] XID: {}\nFlag: {:016b}\n", field_num, self.xid, self.flg));
        }

        Ok(())
    }

    pub fn pdb(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 4, "Size of field {} < 4", reader.data().len());

        if parser.can_dump(1) {
            let pdb_id = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}; PDB] PDB id: {}\n", field_num, pdb_id));
        } else {
            reader.skip_bytes(4);
        }
        
        Ok(())
    }

    pub fn kteop(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 36, "Size of field {} < 36", reader.data().len());

        if parser.can_dump(1) {
            reader.skip_bytes(4);
            let ext = reader.read_u32()?;
            reader.skip_bytes(4);
            let ext_size = reader.read_u32()?;
            let highwater = reader.read_u32()?;
            reader.skip_bytes(4);
            let offset = reader.read_u32()?;

            parser.write_dump(format_args!("\n[Change {}; KTEOP] ext: {} ext size: {} HW: {} offset: {}\n", field_num, ext, ext_size, highwater, offset));
        } else {
            reader.skip_bytes(36);
        }

        Ok(())
    }
}

impl VectorParser for OpCode0502 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<(), OLRError> {
        let mut result = OpCode0502::default();

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.ktudh(parser, vector_header, &mut field_reader, 0)?;
        } else {
            return olr_perr!("Expect ktudh field");
        }

        if parser.version().unwrap() >= constants::REDO_VERSION_12_1 {
            if let Some(mut field_reader) = reader.next_field_reader() {
                if field_reader.data().len() == 4 {
                    result.pdb(parser, vector_header, &mut field_reader, 1)?;
                } else {
                    result.kteop(parser, vector_header, &mut field_reader, 1)?;

                    if let Some(mut field_reader) = reader.next_field_reader() {
                        result.pdb(parser, vector_header, &mut field_reader, 2)?;
                    }
                }
            }
        }
        Ok(())
    }
}
