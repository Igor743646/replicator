use super::{VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, opcodes::opcode0501::OpCode0501, parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader}};

#[derive(Default, Debug)]
pub struct OpCode1102 {

}

impl VectorParser for OpCode1102 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<VectorInfo, OLRError> {
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
            return Ok(VectorInfo::OpCode0501(result));
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
        
        Ok(VectorInfo::OpCode0501(result))
    }
}
