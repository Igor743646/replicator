use std::io::{Error, ErrorKind, Result, Write};

use crate::common::{constants::REDO_VERSION_12_1, types::{TypeRBA, TypeScn, TypeTimestamp}};

use super::parser_impl::{BlockHeader, RedoRecordHeader, RedoRecordHeaderExpansion};

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Endian {
    LittleEndian,
    BigEndian,
    NativEndian,
}

pub struct ByteReader<'a> {
    data : &'a [u8],
    cursor : usize,
    endian : Endian,
}

#[allow(dead_code)]
impl<'a> ByteReader<'a> {
    pub fn from_bytes(data : &'a [u8]) -> Self {
        Self {
            data,
            cursor : 0,
            endian : Endian::LittleEndian,
        }
    }

    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn endian(&self) -> Endian {
        self.endian
    }

    pub fn reset_cursor(&mut self) {
        self.cursor = 0;
    }

    pub fn set_cursor(&mut self, position : usize) -> Result<()> {
        if position > self.data.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "could not set cursor greater than buffer length",
            ));
        }

        self.cursor = position;
        Ok(())
    }

    pub fn set_endian(&mut self, endian : Endian) {
        self.endian = endian;
    }

    pub fn skip_bytes(&mut self, size : usize) {
        self.cursor = std::cmp::min(self.cursor + size, self.data.len());
    }

    #[inline]
    fn validate_size(&self, size : usize) -> Result<()> {
        if self.cursor + size > self.data.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "could not read, not enough bytes",
            ));
        }
        Ok(())
    }

    pub unsafe fn read_u8_unchecked(&mut self) -> u8 {
        let result;
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor);
            result = *ptr;
        }
        self.cursor += 1;
        result
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        self.validate_size(1)?;
        unsafe { Ok(self.read_u8_unchecked()) }
    }

    pub unsafe fn read_u16_unchecked(&mut self) -> u16 {
        let mut result: u16 = 0;
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *const u8;
            match self.endian {
                Endian::LittleEndian => {
                    result |= *ptr as u16;
                    result |= (*ptr.add(1) as u16) << 8;
                },
                Endian::BigEndian => {
                    result |= *ptr as u16;
                    result <<= 8;
                    result |= *ptr.add(1) as u16;
                },
                Endian::NativEndian => {
                    result = *(ptr as *const u16);
                },
            }
        }
        self.cursor += 2;
        result
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        self.validate_size(2)?;
        unsafe { Ok(self.read_u16_unchecked()) }
    }

    pub unsafe fn read_u32_unchecked(&mut self) -> u32 {
        let mut result: u32 = 0;
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *const u8;
            match self.endian {
                Endian::LittleEndian => {
                    result |= *ptr as u32;
                    result |= (*ptr.add(1) as u32) << 8;
                    result |= (*ptr.add(2) as u32) << 16;
                    result |= (*ptr.add(3) as u32) << 24;
                },
                Endian::BigEndian => {
                    result |= *ptr as u32;
                    result <<= 8;
                    result |= *ptr.add(1) as u32;
                    result <<= 8;
                    result |= *ptr.add(2) as u32;
                    result <<= 8;
                    result |= *ptr.add(3) as u32;
                },
                Endian::NativEndian => {
                    result = *(ptr as *const u32);
                },
            }
        }
        self.cursor += 4;
        result
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        self.validate_size(4)?;
        Ok(unsafe { self.read_u32_unchecked() })
    }

    pub unsafe fn read_u64_unchecked(&mut self) -> u64 {
        let mut result: u64 = 0;
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *const u8;
            match self.endian {
                Endian::LittleEndian => {
                    result |= *ptr as u64;
                    result |= (*ptr.add(1) as u64) << 8;
                    result |= (*ptr.add(2) as u64) << 16;
                    result |= (*ptr.add(3) as u64) << 24;
                    result |= (*ptr.add(4) as u64) << 32;
                    result |= (*ptr.add(5) as u64) << 40;
                    result |= (*ptr.add(6) as u64) << 48;
                    result |= (*ptr.add(7) as u64) << 56;
                },
                Endian::BigEndian => {
                    result |= *ptr as u64;
                    result <<= 8;
                    result |= *ptr.add(1) as u64;
                    result <<= 8;
                    result |= *ptr.add(2) as u64;
                    result <<= 8;
                    result |= *ptr.add(3) as u64;
                    result <<= 8;
                    result |= *ptr.add(4) as u64;
                    result <<= 8;
                    result |= *ptr.add(5) as u64;
                    result <<= 8;
                    result |= *ptr.add(6) as u64;
                    result <<= 8;
                    result |= *ptr.add(7) as u64;
                },
                Endian::NativEndian => {
                    result = *(ptr as *const u64);
                },
            }
        }
        self.cursor += 8;
        result
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        self.validate_size(8)?;
        Ok(unsafe { self.read_u64_unchecked() })
    }

    pub unsafe fn read_i8_unchecked(&mut self) -> i8 {
        self.read_u8_unchecked() as i8
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    pub unsafe fn read_i16_unchecked(&mut self) -> i16 {
        self.read_u16_unchecked() as i16
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        Ok(self.read_u16()? as i16)
    }

    pub unsafe fn read_i32_unchecked(&mut self) -> i32 {
        self.read_u32_unchecked() as i32
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        Ok(self.read_u32()? as i32)
    }

    pub unsafe fn read_i64_unchecked(&mut self) -> i64 {
        self.read_u64_unchecked() as i64
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(self.read_u64()? as i64)
    }

    pub unsafe fn read_rba_unchecked(&mut self) -> TypeRBA {
        unsafe { TypeRBA::new(
            self.read_u32_unchecked(),
            self.read_u32_unchecked(),
            self.read_u16_unchecked() & 0x7FFF
        ) }
    }

    pub fn read_rba(&mut self) -> Result<TypeRBA> {
        self.validate_size(10)?;
        Ok( unsafe { self.read_rba_unchecked() } )
    }

    pub fn read_block_header(&mut self) -> Result<BlockHeader> {
        self.validate_size(16)?;

        unsafe {
            let block_flag = self.read_u8_unchecked();
            let file_type = self.read_u8_unchecked();
            self.skip_bytes(2);
            let rba = self.read_rba_unchecked();
            let checksum = self.read_u16_unchecked();
        
            Ok(BlockHeader{
                block_flag,
                file_type,
                rba,
                checksum
            })
        }
    }

    pub unsafe fn read_scn_unchecked(&mut self) -> TypeScn {
        let base : u64 = unsafe { self.read_u32_unchecked() as u64 };
        let wrap1 : u64 = unsafe { self.read_u16_unchecked() as u64 };
        let wrap2 : u64 = unsafe { self.read_u16_unchecked() as u64 };

        if (base | (wrap1 << 32)) == 0xFFFFFFFFFFFF {
            return TypeScn::default();
        }

        let mut res = base;

        if wrap1 & 0x8000 != 0 {
            res |= wrap2 << 32;
            res |= (wrap1 & 0x7FFF) << 48;
        } else {
            res |= wrap1 << 32;
        }

        TypeScn::from(res)
    }

    pub fn read_scn(&mut self) -> Result<TypeScn> {
        self.validate_size(8)?;
        Ok(unsafe { self.read_scn_unchecked() })
    }

    pub unsafe fn read_timestamp_unchecked(&mut self) -> TypeTimestamp {
        unsafe { self.read_u32_unchecked().into() }
    }

    pub fn read_timestamp(&mut self) -> Result<TypeTimestamp> {
        self.validate_size(4)?;
        Ok(unsafe { self.read_timestamp_unchecked() })
    }

    pub fn read_bytes(&mut self, size : usize) -> Result<Vec<u8>> {
        self.validate_size(size)?;
        let mut res = Vec::<u8>::new();
        res.resize(size, 0);
        let range = self.cursor .. self.cursor + size;
        res.copy_from_slice(&self.data[range]);
        self.skip_bytes(size);
        Ok(res)
    }

    pub fn read_redo_record_header(&mut self, version : u32) -> Result<RedoRecordHeader> {
        self.validate_size(24)?;

        let mut result = RedoRecordHeader::default();

        unsafe {
            result.record_size = self.read_u32_unchecked();
            result.vld = self.read_u8_unchecked();
            self.skip_bytes(1);
            result.scn = (((self.read_u16_unchecked() as u64) << 32) |
                            (self.read_u32_unchecked() as u64)).into();
            result.sub_scn = self.read_u16_unchecked();
            self.skip_bytes(2);

            if version >= REDO_VERSION_12_1 {
                result.container_uid = Some(self.read_u32_unchecked());
                self.skip_bytes(4);
            } else {
                self.skip_bytes(8);
            }

            if result.vld & 0x04 != 0 {
                self.validate_size(68)?;
                let mut exp = RedoRecordHeaderExpansion::default();
                exp.record_num = self.read_u16_unchecked();
                exp.record_num_max = self.read_u16_unchecked();
                exp.records_count = self.read_u32_unchecked();
                self.skip_bytes(8);
                exp.records_scn = self.read_scn_unchecked();
                exp.scn1 = self.read_scn_unchecked();
                exp.scn2 = self.read_scn_unchecked();
                exp.records_timestamp = self.read_timestamp()?;
                result.expansion = Some(exp);
            }
        }

        Ok(result)
    }

    pub fn to_hex_dump(&self) -> String {
        let str = "\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let a : String = self.data
            .iter()
            .enumerate()
            .map(|(idx, char)| -> String {
                let color = match (idx, char) {
                    (idx, _) if idx == self.cursor => "\x1b[4;96m",
                    (_, 0) => "\x1b[2;90m",
                    _ => "",
                };
                match (idx % 32, idx % 8) {
                    (_, 7) => format!("{}{:02X}\x1b[0m  ", color, char),
                    (0, _) => format!("\n{:016X}: {}{:02X}\x1b[0m ", idx / 32, color, char),
                    _ => format!("{}{:02X}\x1b[0m ", color, char),
                }
            })
            .collect();

        str + a.as_str()
    }

    pub fn to_error_hex_dump(&self, start : usize, size : usize) -> String {
        debug_assert!(size > 0);
        debug_assert!(start + size <= self.data.len());
        let str = "\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let a : String = self.data
            .iter()
            .enumerate()
            .map(|(idx, char)| -> String {
                let (color, is_end) = match (idx, char) {
                    (idx, _) if start <= idx && idx < start + size => ("\x1b[4;91m", (idx + 1 == start + size)),
                    (idx, _) if idx == self.cursor => ("\x1b[4;96m", true),
                    (_, 0) => ("\x1b[2;90m", true),
                    _ => ("", true),
                };
                match (idx % 32, idx % 8, is_end) {
                    (_, 7, true) => format!("{}{:02X}\x1b[0m  ", color, char),
                    (0, _, true) => format!("\n{:016X}: {}{:02X}\x1b[0m ", idx, color, char),
                    (31, 7, false) => format!("{}{:02X}\x1b[0m  ", color, char),
                    (_, 7, false) => format!("{}{:02X}  \x1b[0m", color, char),
                    (0, _, false) => format!("\n{:016X}: {}{:02X} \x1b[0m", idx, color, char),
                    (_, _, true) => format!("{}{:02X}\x1b[0m ", color, char),
                    (_, _, false) => format!("{}{:02X} \x1b[0m", color, char),
                }
            })
            .collect();

        str + a.as_str()
    }

    pub fn read_bytes_into(&mut self, size : usize, buffer : &mut impl Write) -> Result<()> {
        self.validate_size(size)?;
        let range = self.cursor .. self.cursor + size;
        buffer.write(&self.data[range])?;
        self.skip_bytes(size);
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use crate::parser::byte_reader::Endian;

    use super::ByteReader;
    use std::io::Result;

    #[test]
    fn test_simple() -> Result<()> {
        let buffer: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let mut reader = ByteReader::from_bytes(buffer.as_slice());

        reader.set_endian(Endian::LittleEndian);
        assert_eq!(reader.read_u8()?, 0x11);
        assert_eq!(reader.read_u16()?, 0x3322);

        reader.set_endian(Endian::BigEndian);
        assert_eq!(reader.read_u8()?, 0x44);
        assert_eq!(reader.read_u16()?, 0x5566);
        reader.reset_cursor();

        reader.set_endian(Endian::LittleEndian);
        assert_eq!(reader.read_u32()?, 0x44332211);

        reader.set_endian(Endian::BigEndian);
        assert_eq!(reader.read_u32()?, 0x55667788);
        reader.reset_cursor();

        reader.set_endian(Endian::LittleEndian);
        assert_eq!(reader.read_u64()?, 0x8877665544332211);
        reader.reset_cursor();

        reader.set_endian(Endian::BigEndian);
        assert_eq!(reader.read_u64()?, 0x1122334455667788);

        Ok(())
    }

    #[test]
    fn read_scn() -> Result<()> {
        let buffer: [u8; 16] = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];
        let mut reader = ByteReader::from_bytes(&buffer);

        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), u64::MAX);
        assert_eq!(Into::<u64>::into(scn2), u64::MAX);

        Ok(())
    }

    #[test]
    fn read_scn2_little() -> Result<()> {
        let buffer: [u8; 16] = [0x7A, 0x90, 0xA1, 0x06, 0x55, 0xA4, 0x24, 0x00,   0x7A, 0x90, 0xA1, 0x06, 0x55, 0x24, 0x00, 0x00];
        let mut reader = ByteReader::from_bytes(&buffer);
        reader.set_endian(Endian::LittleEndian);
        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), 0x2455002406A1907A, "left: {}", scn1);
        assert_eq!(Into::<u64>::into(scn2), 0x0000245506A1907A, "left: {}", scn2);

        Ok(())
    }

    #[test]
    fn read_scn2_big() -> Result<()> {
        let buffer: [u8; 16] = [0x7A, 0x90, 0xA1, 0x06, 0x55, 0xA4, 0x00, 0x00,   0x7A, 0x90, 0xA1, 0x06, 0xA5, 0x24, 0x00, 0x24];
        let mut reader = ByteReader::from_bytes(&buffer);
        reader.set_endian(Endian::BigEndian);
        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), 0x000055A47A90A106, "left: {}", scn1);
        assert_eq!(Into::<u64>::into(scn2), 0x252400247A90A106, "left: {}", scn2);

        Ok(())
    }
}
