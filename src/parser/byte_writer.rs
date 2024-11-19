use std::io::{Error, ErrorKind, Result};

use super::byte_reader::Endian;

pub struct ByteWriter<'a> {
    data : &'a mut [u8],
    cursor : usize,
    endian : Endian,
}

#[allow(dead_code)]
impl<'a> ByteWriter<'a> {
    pub fn from_bytes(data : &'a mut [u8]) -> Self {
        Self {
            data,
            cursor : 0,
            endian : Endian::LittleEndian,
        }
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

    pub unsafe fn write_u8_unchecked(&mut self, val : u8) {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(self.cursor);
            *ptr = val;
        }
        self.cursor += 1;
    }

    pub fn write_u8(&mut self, val : u8) -> Result<()> {
        self.validate_size(1)?;
        unsafe { Ok(self.write_u8_unchecked(val)) }
    }

    pub unsafe fn write_u16_unchecked(&mut self, val : u16) {
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *mut u8;
            match self.endian {
                Endian::LittleEndian => {
                    *(ptr) = (val & 0xFF) as u8;
                    *(ptr.add(1)) = ((val >> 8) & 0xFF) as u8;
                },
                Endian::BigEndian => {
                    *(ptr.add(1)) = (val & 0xFF) as u8;
                    *(ptr) = ((val >> 8) & 0xFF) as u8;
                },
                Endian::NativEndian => {
                    *(ptr as *mut u16) = val;
                },
            }
        }
        self.cursor += 2;
    }

    pub fn write_u16(&mut self, val : u16) -> Result<()> {
        self.validate_size(2)?;
        unsafe { Ok(self.write_u16_unchecked(val)) }
    }

    pub unsafe fn write_u32_unchecked(&mut self, val : u32) {
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *mut u8;
            match self.endian {
                Endian::LittleEndian => {
                    *(ptr) = (val & 0xFF) as u8;
                    *(ptr.add(1)) = ((val >> 8) & 0xFF) as u8;
                    *(ptr.add(2)) = ((val >> 16) & 0xFF) as u8;
                    *(ptr.add(3)) = ((val >> 24) & 0xFF) as u8;
                },
                Endian::BigEndian => {
                    *(ptr.add(3)) = (val & 0xFF) as u8;
                    *(ptr.add(2)) = ((val >> 8) & 0xFF) as u8;
                    *(ptr.add(1)) = ((val >> 16) & 0xFF) as u8;
                    *(ptr) = ((val >> 24) & 0xFF) as u8;
                },
                Endian::NativEndian => {
                    *(ptr as *mut u32) = val;
                },
            }
        }
        self.cursor += 4;
    }

    pub fn write_u32(&mut self, val : u32) -> Result<()> {
        self.validate_size(4)?;
        Ok(unsafe { self.write_u32_unchecked(val) })
    }

    pub unsafe fn write_u64_unchecked(&mut self, val : u64) {
        unsafe {
            let ptr = self.data.as_ptr().add(self.cursor) as *mut u8;
            match self.endian {
                Endian::LittleEndian => {
                    *(ptr) = (val & 0xFF) as u8;
                    *(ptr.add(1)) = ((val >> 8) & 0xFF) as u8;
                    *(ptr.add(2)) = ((val >> 16) & 0xFF) as u8;
                    *(ptr.add(3)) = ((val >> 24) & 0xFF) as u8;
                    *(ptr.add(4)) = ((val >> 32) & 0xFF) as u8;
                    *(ptr.add(5)) = ((val >> 40) & 0xFF) as u8;
                    *(ptr.add(6)) = ((val >> 48) & 0xFF) as u8;
                    *(ptr.add(7)) = ((val >> 56) & 0xFF) as u8;
                },
                Endian::BigEndian => {
                    *(ptr.add(7)) = (val & 0xFF) as u8;
                    *(ptr.add(6)) = ((val >> 8) & 0xFF) as u8;
                    *(ptr.add(5)) = ((val >> 16) & 0xFF) as u8;
                    *(ptr.add(4)) = ((val >> 24) & 0xFF) as u8;
                    *(ptr.add(3)) = ((val >> 32) & 0xFF) as u8;
                    *(ptr.add(2)) = ((val >> 40) & 0xFF) as u8;
                    *(ptr.add(1)) = ((val >> 48) & 0xFF) as u8;
                    *(ptr) = ((val >> 56) & 0xFF) as u8;
                },
                Endian::NativEndian => {
                    *(ptr as *mut u64) = val;
                },
            }
        }
        self.cursor += 8;
    }

    pub fn write_u64(&mut self, val : u64) -> Result<()> {
        self.validate_size(8)?;
        Ok(unsafe { self.write_u64_unchecked(val) })
    }

    pub unsafe fn write_i8_unchecked(&mut self, val : i8) {
        self.write_u8_unchecked(val as u8)
    }

    pub fn write_i8(&mut self, val : i8) -> Result<()> {
        self.write_u8(val as u8)
    }

    pub unsafe fn write_i16_unchecked(&mut self, val : i16) {
        self.write_u16_unchecked(val as u16)
    }

    pub fn write_i16(&mut self, val : i16) -> Result<()> {
        self.write_u16(val as u16)
    }

    pub unsafe fn write_i32_unchecked(&mut self, val : i32) {
        self.write_u32_unchecked(val as u32)
    }

    pub fn write_i32(&mut self, val : i32) -> Result<()> {
        self.write_u32(val as u32)
    }

    pub unsafe fn write_i64_unchecked(&mut self, val : i64) {
        self.write_u64_unchecked(val as u64)
    }

    pub fn write_i64(&mut self, val : i64) -> Result<()> {
        self.write_u64(val as u64)
    }

    pub fn write_bytes(&mut self, bytes : &[u8]) -> Result<()> {
        self.validate_size(bytes.len())?;
        let range = self.cursor .. self.cursor + bytes.len();
        self.data[range].copy_from_slice(bytes);
        self.skip_bytes(bytes.len());
        Ok(())
    }
}
