use crate::common::errors::OLRError;

use super::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}};
pub mod opcode0502;

pub trait VectorParser {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError>;
}