use std::fmt::Display;

use opcode0501::OpCode0501;

use crate::common::errors::OLRError;

use super::{parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader};
pub mod opcode0502;
pub mod opcode0520;
pub mod opcode0504;
pub mod opcode0501;
pub mod opcode1102;

#[derive(Debug)]
pub enum VectorInfo<'a> {
    OpCode0501(opcode0501::OpCode0501<'a>),
    OpCode0502(opcode0502::OpCode0502<'a>),
    OpCode0504(opcode0504::OpCode0504<'a>),
    OpCode0520(opcode0520::OpCode0520<'a>),
    OpCode1102(opcode1102::OpCode1102<'a>),
}

impl<'a> Display for VectorInfo<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            VectorInfo::OpCode0501(_) => "opcode0501",
            VectorInfo::OpCode0502(_) => "opcode0502",
            VectorInfo::OpCode0504(_) => "opcode0504",
            VectorInfo::OpCode0520(_) => "opcode0520",
            VectorInfo::OpCode1102(_) => "opcode1102",
        })
    }
}

pub trait VectorParser<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorInfo<'a>, OLRError>;
}
