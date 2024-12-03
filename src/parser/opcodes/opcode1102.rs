use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, opcodes::opcode0501::OpCode0501, parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader}};

#[derive(Default)]
pub struct OpCode1102 {
}

impl OpCode1102 {
    pub fn ktudh(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 32, "Size of field {} != 32", reader.data().len());


        Ok(())
    }

    pub fn pdb(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 4, "Size of field {} != 4", reader.data().len());

        if parser.can_dump(1) {
            let pdb_id = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}; PDB] PDB id: {}\n", field_num, pdb_id));
        } else {
            reader.skip_bytes(4);
        }
        
        Ok(())
    }
}

impl VectorParser for OpCode1102 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<(), OLRError> {
        let mut result = OpCode0501::default();

        if let Some(mut field_reader) = reader.next() {
            result.ktb_redo(parser, vector_header, &mut field_reader, 0)?;
        } else {
            return olr_perr!("Expect ktb_redo field");
        }

        let mut ktb_opcode_reader = if let Some(mut field_reader) = reader.next() {
            result.kdo_opcode(parser, vector_header, &mut field_reader, 1)?;
            field_reader
        } else {
            return Ok(());
        };

        if let Some(mut field_reader) = reader.next() {
            if field_reader.data().len() == result.size_delt as usize && result.cc != 1 {
                std::unimplemented!("Compressed data");
            } else {
                let mut nulls: u8 = 0;
                for i in 0 .. result.cc {
                    let mask = 1u8 << (i & 0b111);
                    if mask == 1 {
                        nulls = ktb_opcode_reader.read_u8()?;
                    }
                    
                    if i > 0 {
                        field_reader = reader.next().unwrap();
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
}
