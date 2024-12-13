use std::fmt::Write;

use crate::{common::{constants::{self, REDO_VERSION_12_1}, errors::OLRError, types::{TypeRBA, TypeScn, TypeTimestamp, TypeUba}}, olr_perr};

use super::archive_structs::{block_header::BlockHeader, record_header::{RecordHeader, RecordHeaderExpansion}, vector_header::{VectorHeader, VectorHeaderExpansion}};

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Endian {
    LittleEndian,
    BigEndian,
    NativEndian,
}

#[derive(Copy, Clone)]
pub struct ByteReader<'a> {
    data : &'a [u8],
    cursor : usize,
    endian : Endian,
}

macro_rules! number_unsafe_impl {
    (
        Self = $SelfType:ty,
        $FuncName:ident,
        $NumType:ty 
    ) => {
        pub unsafe fn $FuncName(&mut self) -> $NumType {
            let result: $NumType = unsafe {
                let ptr = self.data.as_ptr().add(self.cursor) as *const [u8; size_of::<$NumType>()];
                match self.endian {
                    Endian::LittleEndian => <$NumType>::from_le_bytes(*ptr),
                    Endian::BigEndian => <$NumType>::from_be_bytes(*ptr),
                    Endian::NativEndian => <$NumType>::from_ne_bytes(*ptr),
                }
            };
            self.cursor += size_of::<$NumType>();
            result
        }
    }
}

macro_rules! number_impl {
    (
        Self = $SelfType:ty,
        $FuncName:ident,
        $UnsafeFuncName:ident,
        $NumType:ty 
    ) => {
        pub fn $FuncName(&mut self) -> Result<$NumType, OLRError> {
            self.validate_size(size_of::<$NumType>())?;
            unsafe { Ok(self.$UnsafeFuncName()) }
        }
    }
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

    pub fn set_cursor(&mut self, position : usize) -> Result<(), OLRError> {
        if position > self.data.len() {
            return olr_perr!("Could not set cursor greater than buffer length");
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

    pub fn align_up(&mut self, size : usize) {
        assert!(size.is_power_of_two());
        self.cursor = (self.cursor + (size - 1)) & !(size - 1);
    }

    pub fn eof(&mut self) -> bool {
        self.cursor >= self.data.len()
    }

    #[inline]
    fn validate_size(&self, size : usize) -> Result<(), OLRError> {
        if self.cursor + size > self.data.len() {
            return olr_perr!("Could not read, not enough bytes. Dump:\x1b[0m {}", self.to_hex_dump());
        }
        Ok(())
    }

    number_unsafe_impl!{Self = ByteReader, read_u8_unchecked, u8}
    number_unsafe_impl!{Self = ByteReader, read_u16_unchecked, u16}
    number_unsafe_impl!{Self = ByteReader, read_u32_unchecked, u32}
    number_unsafe_impl!{Self = ByteReader, read_u64_unchecked, u64}
    number_unsafe_impl!{Self = ByteReader, read_i8_unchecked, i8}
    number_unsafe_impl!{Self = ByteReader, read_i16_unchecked, i16}
    number_unsafe_impl!{Self = ByteReader, read_i32_unchecked, i32}
    number_unsafe_impl!{Self = ByteReader, read_i64_unchecked, i64}
    
    number_impl!{Self = ByteReader, read_u8, read_u8_unchecked, u8}
    number_impl!{Self = ByteReader, read_u16, read_u16_unchecked, u16}
    number_impl!{Self = ByteReader, read_u32, read_u32_unchecked, u32}
    number_impl!{Self = ByteReader, read_u64, read_u64_unchecked, u64}
    number_impl!{Self = ByteReader, read_i8, read_i8_unchecked, i8}
    number_impl!{Self = ByteReader, read_i16, read_i16_unchecked, i16}
    number_impl!{Self = ByteReader, read_i32, read_i32_unchecked, i32}
    number_impl!{Self = ByteReader, read_i64, read_i64_unchecked, i64}

    pub unsafe fn read_rba_unchecked(&mut self) -> TypeRBA {
        unsafe { TypeRBA::new(
            self.read_u32_unchecked(),
            self.read_u32_unchecked(),
            self.read_u16_unchecked() & 0x7FFF
        ) }
    }

    pub fn read_rba(&mut self) -> Result<TypeRBA, OLRError> {
        self.validate_size(10)?;
        Ok( unsafe { self.read_rba_unchecked() } )
    }

    pub fn read_block_header(&mut self) -> Result<BlockHeader, OLRError> {
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

    pub unsafe fn read_uba_unchecked(&mut self) -> TypeUba {
        let temp : u64 = unsafe { self.read_u64_unchecked() };
        TypeUba::new(temp)
    }

    pub fn read_uba(&mut self) -> Result<TypeUba, OLRError> {
        self.validate_size(8)?;
        Ok(unsafe { self.read_uba_unchecked() })
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

    pub fn read_scn(&mut self) -> Result<TypeScn, OLRError> {
        self.validate_size(8)?;
        Ok(unsafe { self.read_scn_unchecked() })
    }

    pub unsafe fn read_timestamp_unchecked(&mut self) -> TypeTimestamp {
        unsafe { self.read_u32_unchecked().into() }
    }

    pub fn read_timestamp(&mut self) -> Result<TypeTimestamp, OLRError> {
        self.validate_size(4)?;
        Ok(unsafe { self.read_timestamp_unchecked() })
    }

    pub fn read_bytes(&mut self, size : usize) -> Result<Vec<u8>, OLRError> {
        self.validate_size(size)?;
        let mut res = Vec::<u8>::new();
        res.resize(size, 0);
        let range = self.cursor .. self.cursor + size;
        res.copy_from_slice(&self.data[range]);
        self.skip_bytes(size);
        Ok(res)
    }

    pub fn read_redo_record_header(&mut self, version : u32) -> Result<RecordHeader, OLRError> {
        self.validate_size(24)?;

        let mut result = RecordHeader::default();

        unsafe {
            result.record_size = self.read_u32_unchecked();
            result.vld = self.read_u8_unchecked();
            self.skip_bytes(1);
            result.scn = (((self.read_u16_unchecked() as u64) << 32) |
                            (self.read_u32_unchecked() as u64)).into();
            result.sub_scn = self.read_u16_unchecked();
            self.skip_bytes(2);

            if version >= REDO_VERSION_12_1 {
                result.container_id = Some(self.read_u32_unchecked());
                self.skip_bytes(4);
            } else {
                self.skip_bytes(8);
            }

            if result.vld & 0x04 != 0 {
                self.validate_size(68)?;
                let mut exp = RecordHeaderExpansion::default();
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

    pub fn read_redo_vector_header(&mut self, version : u32) -> Result<VectorHeader, OLRError> {
        if version >= constants::REDO_VERSION_12_1 {
            self.validate_size(24 + 8 + 2)?;
        } else {
            self.validate_size(24 + 2)?;
        }

        let mut result = VectorHeader::default();

        unsafe {
            result.op_code.0 = self.read_u8_unchecked();
            result.op_code.1 = self.read_u8_unchecked();
            result.class = self.read_u16_unchecked();
            result.afn = self.read_u16_unchecked();
            self.skip_bytes(2);
            result.dba = self.read_u32_unchecked();
            result.vector_scn = self.read_scn_unchecked();
            result.seq = self.read_u8_unchecked();
            result.typ = self.read_u8_unchecked();
            self.skip_bytes(2);

            if version >= constants::REDO_VERSION_12_1 {
                let mut ext = VectorHeaderExpansion::default();
                ext.container_id = self.read_u16_unchecked();
                self.skip_bytes(2);
                ext.flag = self.read_u16_unchecked();
                self.skip_bytes(2);
                result.expansion = Some(ext);
            }

            result.fields_count = (self.read_u16_unchecked() - 2) / 2;
            result.fields_sizes = (0 .. result.fields_count)
                .map(|_| self.read_u16())
                .collect::<Result<Vec<u16>, OLRError>>()?;
        }

        Ok(result)
    }

    pub fn to_colorless_hex_dump(&self) -> String {
        let str = "\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let a : String = self.data
            .iter()
            .enumerate()
            .map(|(idx, char)| -> String {
                match (idx % 32, idx % 8) {
                    (_, 7) => format!("{:02X}  ", char),
                    (0, _) => format!("\n{:016X}: {:02X} ", idx / 32, char),
                    _ => format!("{:02X} ", char),
                }
            })
            .collect();

        str + a.as_str()
    }

    pub fn to_hex_dump(&self) -> String {
        let str = "\x1b[0m\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
        let a : String = self.data
                .chunks(32)
                .enumerate()
                .map(|(row_id, bytes)| -> String {
                    let mut row = String::with_capacity(150);
                    row.write_fmt(format_args!("\n{:016X}: ", row_id * 32)).unwrap();

                    for (col_id, byte) in bytes.iter().enumerate() {
                        let color = match (col_id + 32 * row_id, byte) {
                            (idx, _) if idx == self.cursor => "\x1b[4;96m",
                            (_, 0) => "\x1b[2;90m",
                            _ => "",
                        };

                        match col_id % 8 {
                            7 => row.write_fmt(format_args!("{}{:02X}\x1b[0m  ", color, byte)).unwrap(),
                            _ => row.write_fmt(format_args!("{}{:02X}\x1b[0m ", color, byte)).unwrap(),
                        }
                    }

                    if bytes.len() < 32 {
                        row.write_str(format!("{}", " ".repeat(3 * (32 - bytes.len()) + (32 - bytes.len()) / 8  + 1) ).as_str()).unwrap();
                    }

                    for byte in bytes {
                        match char::from_u32(*byte as u32) {
                            Some(chr) if chr.is_ascii_alphanumeric() => row.write_char(chr).unwrap(),
                            Some(_) => row.write_char('.').unwrap(),
                            _ => row.write_char('.').unwrap(),
                        }
                    }

                    row
                })
                .collect();
        str + a.as_str() + "\n"
    }

    pub fn to_error_hex_dump(&self, start : usize, size : usize) -> String {
        debug_assert!(size > 0);
        debug_assert!(start + size <= self.data.len());
        let str = "\x1b[0m\n                  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F".to_string();
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

    pub fn read_bytes_into(&mut self, size : usize, buffer : &mut [u8]) -> Result<(), OLRError> {
        self.validate_size(size)?;
        
        if buffer.len() < size {
            return olr_perr!("Could not write in buffer with not enough capacity");
        }

        let range = self.cursor .. self.cursor + size;
        for (idx, byte) in (0..size).zip(self.data[range].into_iter()) {
            buffer[idx] = *byte;
        }
        self.skip_bytes(size);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{common::errors::OLRError, parser::byte_reader::Endian};

    use super::ByteReader;

    #[test]
    fn test_simple() -> Result<(), OLRError> {
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
    fn read_scn() -> Result<(), OLRError> {
        let buffer: [u8; 16] = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];
        let mut reader = ByteReader::from_bytes(&buffer);

        let scn1 = reader.read_scn()?;
        let scn2 = reader.read_scn()?;

        assert_eq!(Into::<u64>::into(scn1), u64::MAX);
        assert_eq!(Into::<u64>::into(scn2), u64::MAX);

        Ok(())
    }

    #[test]
    fn read_scn2_little() -> Result<(), OLRError> {
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
    fn read_scn2_big() -> Result<(), OLRError> {
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
