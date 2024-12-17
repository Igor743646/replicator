use crate::{common::errors::Result, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};


pub mod ktudh;
pub mod kteop;
pub mod pdb;
pub mod ktbredo;
pub mod kdoopcode;
pub mod ktucm;
pub mod ktucf;
pub mod ktudb;
pub mod ktub;

pub trait VectorField {
    fn parse_from_reader(
        parser : &mut Parser, 
        vec_reader : &mut VectorReader,
        reader : &mut ByteReader, 
        field_num : usize) -> Result<Self> where Self: Sized;
}
