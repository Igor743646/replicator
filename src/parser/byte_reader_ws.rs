use std::ops::{Deref, DerefMut};

use bytebuffer::ByteReader;
use crate::common::constants::REDO_VERSION_12_1;
use crate::common::types::TypeTimestamp;
use crate::common::types::{TypeRBA, TypeScn};

use super::parser_impl::{BlockHeader, RedoRecordHeader, RedoRecordHeaderExpansion};
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

    pub fn read_scn(&mut self) -> Result<TypeScn> {
        let base : u64 = self.read_u32()? as u64;
        let wrap1 : u64 = self.read_u16()? as u64;
        let wrap2 : u64 = self.read_u16()? as u64;

        if (base | (wrap1 << 32)) == 0xFFFFFFFFFFFF {
            return Ok(TypeScn::default());
        }

        let mut res = base;

        if wrap1 & 0x8000 != 0 {
            res |= wrap2 << 32;
            res |= (wrap1 & 0x7FFF) << 48;
        } else {
            res |= wrap1 << 32;
        }

        Ok(TypeScn::from(res))
    }

    pub fn read_timestamp(&mut self) -> Result<TypeTimestamp> {
        Ok(self.read_u32()?.into())
    }

    pub fn read_redo_record_header(&mut self, version : u32) -> Result<RedoRecordHeader> {
        let mut result = RedoRecordHeader::default();
        
        result.record_size = self.read_u32()?;
        result.vld = self.read_u8()?;
        self.skip_bytes(1);
        result.scn = (((self.read_u16()? as u64) << 32) |
                        (self.read_u32()? as u64)).into();
        result.sub_scn = self.read_u16()?;
        self.skip_bytes(2);

        if version >= REDO_VERSION_12_1 {
            result.container_uid = Some(self.read_u32()?);
            self.skip_bytes(4);
        } else {
            self.skip_bytes(8);
        }

        if result.vld & 0x04 != 0 {
            let mut exp = RedoRecordHeaderExpansion::default();
            exp.record_num = self.read_u16()?;
            exp.record_num_max = self.read_u16()?;
            exp.records_count = self.read_u32()?;
            self.skip_bytes(8);
            exp.records_scn = self.read_scn()?;
            exp.scn1 = self.read_scn()?;
            exp.scn2 = self.read_scn()?;
            exp.records_timestamp = self.read_timestamp()?;
            result.expansion = Some(exp);
        }

        Ok(result)
    }

    pub fn from_bytes(bytes: &'a [u8]) -> ByteReaderWithSkip {
        Self(ByteReader::from_bytes(bytes))
    }

    pub fn skip_bytes(&mut self, size : usize) {
        let mut rpos = self.0.get_rpos();
        rpos += size;
        self.0.set_rpos(rpos);
    }

    #[allow(dead_code)]
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
            if cnt % 32 == 0 {
                str += &format!("\n{:016X}: ", cnt);
            }
            if start == cnt {
                str += "\x1b[4;91m";
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

#[cfg(test)]
mod test_brws {
    use super::ByteReaderWithSkip;
    use std::io::Result;

    #[test]
    fn read_scn() -> Result<()> {
        let buffer: [u8; 16] = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];
        let mut reader = ByteReaderWithSkip::from_bytes(&buffer);

        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), u64::MAX);
        assert_eq!(Into::<u64>::into(scn2), u64::MAX);

        Ok(())
    }

    #[test]
    fn read_scn2_little() -> Result<()> {
        let buffer: [u8; 16] = [0x7A, 0x90, 0xA1, 0x06, 0x55, 0xA4, 0x24, 0x00,   0x7A, 0x90, 0xA1, 0x06, 0x55, 0x24, 0x00, 0x00];
        let mut reader = ByteReaderWithSkip::from_bytes(&buffer);
        reader.set_endian(bytebuffer::Endian::LittleEndian);
        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), 0x2455002406A1907A, "left: {}", scn1);
        assert_eq!(Into::<u64>::into(scn2), 0x0000245506A1907A, "left: {}", scn2);

        Ok(())
    }

    #[test]
    fn read_scn2_big() -> Result<()> {
        let buffer: [u8; 16] = [0x7A, 0x90, 0xA1, 0x06, 0x55, 0xA4, 0x00, 0x00,   0x7A, 0x90, 0xA1, 0x06, 0xA5, 0x24, 0x00, 0x24];
        let mut reader = ByteReaderWithSkip::from_bytes(&buffer);
        reader.set_endian(bytebuffer::Endian::BigEndian);
        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), 0x000055A47A90A106, "left: {}", scn1);
        assert_eq!(Into::<u64>::into(scn2), 0x252400247A90A106, "left: {}", scn2);

        Ok(())
    }

}
