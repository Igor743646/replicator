use crate::common::errors::OLRError;

use super::{parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader};
pub mod opcode0502;
pub mod opcode0520;
pub mod opcode0504;
pub mod opcode0501;

pub trait VectorParser {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<(), OLRError>;
}
