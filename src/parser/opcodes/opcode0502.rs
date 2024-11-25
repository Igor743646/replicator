use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}}};

#[derive(Default)]
pub struct OpCode0502 {
    pub xid : TypeXid,
    pub flg : u16,
}

impl OpCode0502 {
    pub fn ktudh(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 32, "Size of field {} < 32", vector_header.fields_sizes[field_num]);

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

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 32);
        reader.align_up(4);
        Ok(())
    }

    pub fn pdb(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 4, "Size of field {} < 4", vector_header.fields_sizes[field_num]);

        if parser.can_dump(1) {
            let pdb_id = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}; PDB] PDB id: {}\n", field_num, pdb_id));
        } else {
            reader.skip_bytes(4);
        }
        
        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 4);
        reader.align_up(4);
        Ok(())
    }

    pub fn kteop(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 36, "Size of field {} < 36", vector_header.fields_sizes[field_num]);

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

        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 36);
        reader.align_up(4);
        Ok(())
    }
}

impl VectorParser for OpCode0502 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(vector_header.fields_count <= 3, "Count of fields ({}) > 3", vector_header.fields_count);
        let mut result = OpCode0502::default();

        result.ktudh(parser, vector_header, reader, 0)?;

        if parser.version().unwrap() >= constants::REDO_VERSION_12_1 {
            if vector_header.fields_count >= 2 {
                if vector_header.fields_sizes[1] == 4 {
                    result.pdb(parser, vector_header, reader, 1)?;
                } else {
                    result.kteop(parser, vector_header, reader, 1)?;

                    if vector_header.fields_count >= 3 {
                        result.pdb(parser, vector_header, reader, 2)?;
                    }
                }
            }
        }
        Ok(())
    }
}
