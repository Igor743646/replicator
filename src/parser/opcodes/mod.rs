use crate::{common::errors::OLRError, olr_perr};

use super::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}};
pub mod opcode0502;
pub mod opcode0520;
pub mod opcode0504;
pub mod opcode0501;

pub trait VectorParser {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError>;
}

pub(crate) fn verify_fields_count(reader : &ByteReader, vector_header : &RedoVectorHeader, count : u16) -> Result<(), OLRError> {
    if vector_header.fields_count > count {
        return olr_perr!("Count of fields ({}) > {}. Dump: {}", vector_header.fields_count, count, reader.to_hex_dump());
    }
    
    Ok(())
}
