use std::ops::{Deref, DerefMut};

use bytebuffer::ByteReader;

use crate::common::types::TypeRBA;

use super::parser_impl::BlockHeader;
use std::io::Result;

pub struct ByteReaderWithSkip<'a>(ByteReader<'a>);

impl<'a> Deref for ByteReaderWithSkip<'a> {
    type Target = ByteReader<'a>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for ByteReaderWithSkip<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> ByteReaderWithSkip<'a> {

    pub fn read_rba(&mut self) -> Result<TypeRBA> {
        let block_number = self.read_u32()?;
        let sequence = self.read_u32()?;
        let offset = self.read_u16()? & 0x7FFF;
        Ok(TypeRBA::new(
            block_number,
            sequence,
            offset
        ))
    }

    pub fn read_block_header(&mut self) -> Result<BlockHeader> {
        let block_flag = self.read_u8()?;
        let file_type = self.read_u8()?;
        self.skip_bytes(2);
        let rba = self.read_rba()?;
        let checksum = self.read_u16()?;

        Ok(BlockHeader{
            block_flag,
            file_type,
            rba,
            checksum
        })
    }

    pub fn from_bytes(bytes: &'a [u8]) -> ByteReaderWithSkip {
        Self(ByteReader::from_bytes(bytes))
    }

    pub fn skip_bytes(&mut self, size : usize) {
        let mut rpos = self.0.get_rpos();
        rpos += size;
        self.0.set_rpos(rpos);
    }

    pub fn to_hex_dump(&self) -> String {
        let mut str = "\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let mut cnt: usize = 0;
        let cur_pos = self.get_rpos();

        for b in self.0.as_bytes() {
            if cnt % 32 == 0 {
                str += &format!("\n{:016X}: ", cnt);
            }
            if cnt == cur_pos {
                str += "\x1b[4;96m";
                str += &format!("{:01$X}", b, 2);
                str += "\x1b[0m";
            } else {
                if *b == 0 {
                    str += &format!("\x1b[2m{:01$X}\x1b[0m", b, 2);
                } else {
                    str += &format!("{:01$X}", b, 2);
                }
            }
            str += [" ", "  "][((cnt + 1) % 8 == 0) as usize];
            cnt += 1;
        }
        str.pop();
        str
    }

    pub fn to_error_hex_dump(&self, start : usize, size : usize) -> String {
        debug_assert!(start < self.0.as_bytes().len());
        debug_assert!(size > 0);

        let mut str = "\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let mut cnt: usize = 0;
        
        for b in self.0.as_bytes() {
            if start == cnt {
                str += "\x1b[4;91m";
            }
            if cnt % 32 == 0 {
                str += &format!("\n{:016X}: ", cnt);
            }
            if *b == 0 && (cnt < start || cnt > start + size) {
                str += &format!("\x1b[2m{:01$X}\x1b[0m", b, 2);
            } else {
                str += &format!("{:01$X}", b, 2);
            }
            if start + size - 1 == cnt {
                str += "\x1b[0m";
            }
            str += [" ", "  "][((cnt + 1) % 8 == 0) as usize];
            cnt += 1;
        }
        str.pop();
        str
    }
}
