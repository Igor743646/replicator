use crate::common::errors::OLRError;

use super::{parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader};
pub mod opcode0502;
pub mod opcode0520;
pub mod opcode0504;
pub mod opcode0501;
pub mod opcode1102;

#[derive(Debug)]
pub enum VectorInfo {
    OpCode0501(opcode0501::OpCode0501),
    OpCode0502(opcode0502::OpCode0502),
    OpCode0504(opcode0504::OpCode0504),
    OpCode0520(opcode0520::OpCode0520),
    OpCode1102(opcode1102::OpCode1102),
}

pub trait VectorParser {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<VectorInfo, OLRError>;
}
