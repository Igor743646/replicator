use std::fmt::{Display, Formatter};

use log::{trace, warn};
use opcode0501::OpCode0501;
use opcode0502::OpCode0502;
use opcode0504::OpCode0504;
use opcode0520::OpCode0520;
use opcode1102::OpCode1102;

use crate::common::{errors::Result, types::TypeXid};

use super::{archive_structs::vector_header::VectorHeader, byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader};
pub mod fields;
pub mod opcode0502;
pub mod opcode0520;
pub mod opcode0504;
pub mod opcode0501;
pub mod opcode1102;

#[derive(Debug)]
pub struct Vector<'a> {
    header : VectorHeader,
    data : VectorData<'a>,

    size : usize,
    data_ptr : *const u8,
}

impl<'a> Vector<'a> {
    pub fn parse(parser : &mut Parser, reader : &mut ByteReader<'a>, version : u32) -> Result<Self> {
        let start_position = reader.cursor();
        let data_ptr = reader.data().as_ptr();

        let header: VectorHeader = reader.read_redo_vector_header(version)?;
        trace!("Analize vector: {:?} offset: {}", header.op_code, reader.cursor());
        reader.align_up(4);

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n{}", header))?;
        }

        let body_size : usize = header.fields_sizes
            .iter()
            .map(|x| ((*x + 3) & !3) as usize )
            .sum();

        let vec_reader = VectorReader::new(
            header.clone(), 
            &reader.data()[reader.cursor() .. reader.cursor() + body_size]
        );

        reader.skip_bytes(body_size);

        let size = reader.cursor() - start_position;

        let data = match header.op_code {
            (5, 1) => OpCode0501::parse(parser, vec_reader)?,
            (5, 2) => OpCode0502::parse(parser, vec_reader)?,
            (5, 4) => OpCode0504::parse(parser, vec_reader)?,
            (5, 20) => OpCode0520::parse(parser, vec_reader)?,
            (11, 2) => OpCode1102::parse(parser, vec_reader)?,
            (5, 6) | (5, 11) | (5, 19) | (10, 2) |
            (10, 8) | (10, 18) | (11, 3) | (11, 4) |
            (11, 5) | (11, 6) | (11, 8) | (11, 11) |
            (11, 12) | (11, 16) | (11, 22) | (19, 1) |
            (26, 2) | (26, 6) | (24, 1) => {
                warn!("Opcode: {}.{} not implemented", header.op_code.0, header.op_code.1); 
                VectorData::UnknownOpcode
            },
            (_, _) => {
                VectorData::UnknownOpcode
            },
        };

        Ok(Self {
            header,
            data,
            size,
            data_ptr,
        })
    }
}

#[derive(Debug)]
pub enum VectorKind {
    OpCode0501,
    OpCode0502,
    OpCode0504,
    OpCode0520,
    OpCode1102,
    UnknownOpcode,
}

#[derive(Debug)]
pub enum VectorData<'a> {
    OpCode0501(opcode0501::OpCode0501<'a>),
    OpCode0502(opcode0502::OpCode0502<'a>),
    OpCode0504(opcode0504::OpCode0504<'a>),
    OpCode0520(opcode0520::OpCode0520<'a>),
    OpCode1102(opcode1102::OpCode1102<'a>),
    UnknownOpcode,
}

impl<'a> Vector<'a> {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn opcode(&self) -> u16 {
        ((self.header.op_code.0 as u16) << 8) | (self.header.op_code.1 as u16)
    }

    pub fn data(&self) -> *const u8 {
        self.data_ptr
    }
    
    pub fn kind(&self) -> VectorKind {
        match &self.data {
            VectorData::OpCode0501(_) => VectorKind::OpCode0501,
            VectorData::OpCode0502(_) => VectorKind::OpCode0502,
            VectorData::OpCode0504(_) => VectorKind::OpCode0504,
            VectorData::OpCode0520(_) => VectorKind::OpCode0520,
            VectorData::OpCode1102(_) => VectorKind::OpCode1102,
            VectorData::UnknownOpcode => VectorKind::UnknownOpcode,
        }
    }

    pub fn xid(&self) -> Option<TypeXid> {
        match &self.data {
            VectorData::OpCode0501(inside) => Some(inside.xid),
            VectorData::OpCode0502(inside) => Some(inside.xid),
            VectorData::OpCode0504(inside) => Some(inside.xid),
            VectorData::OpCode0520(_) => None,
            VectorData::OpCode1102(inside) => Some(inside.xid),
            VectorData::UnknownOpcode => None,
        }
    }

    pub fn obj(&self) -> Option<u32> {
        match &self.data {
            VectorData::OpCode0501(inside) => Some(inside.obj),
            VectorData::OpCode0502(_) => None,
            VectorData::OpCode0504(_) => None,
            VectorData::OpCode0520(_) => None,
            VectorData::OpCode1102(_) => None,
            VectorData::UnknownOpcode => None,
        }
    }
}

impl<'a> Display for VectorData<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            VectorData::OpCode0501(_) => "opcode0501",
            VectorData::OpCode0502(_) => "opcode0502",
            VectorData::OpCode0504(_) => "opcode0504",
            VectorData::OpCode0520(_) => "opcode0520",
            VectorData::OpCode1102(_) => "opcode1102",
            VectorData::UnknownOpcode => "unknown opcode",
        })
    }
}

pub trait VectorParser<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorData<'a>>;
}
