pub mod charset;
pub mod charset_7bit;

use charset::CharacterMapper;
use charset_7bit::CharSet7Bit;

use crate::{common::errors::OLRError, olr_err};


#[derive(Default)]
pub struct Locales {
    // pub character_map   : HashMap<u64, Arc<dyn CharacterMapper>>,
    // pub timezone_map    : HashMap<u16, &'static str>,
}

impl std::fmt::Debug for Locales {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Locales")
    }
}

impl Locales {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_char_set(charset_id : u64) -> Result<Box<dyn CharacterMapper>, OLRError> {
        match charset_id {
            // 7-bit charsets
            1 => Ok(Box::new(CharSet7Bit::new("US7ASCII", charset_7bit::UNICODE_MAP_US7ASCII))),
            11 => Ok(Box::new(CharSet7Bit::new("D7DEC", charset_7bit::UNICODE_MAP_D7DEC))),
            13 => Ok(Box::new(CharSet7Bit::new("S7DEC", charset_7bit::UNICODE_MAP_S7DEC))),
            14 => Ok(Box::new(CharSet7Bit::new("E7DEC", charset_7bit::UNICODE_MAP_E7DEC))),
            15 => Ok(Box::new(CharSet7Bit::new("SF7ASCII", charset_7bit::UNICODE_MAP_SF7ASCII))),
            16 => Ok(Box::new(CharSet7Bit::new("NDK7DEC", charset_7bit::UNICODE_MAP_NDK7DEC))),
            17 => Ok(Box::new(CharSet7Bit::new("I7DEC", charset_7bit::UNICODE_MAP_I7DEC))),
            21 => Ok(Box::new(CharSet7Bit::new("SF7DEC", charset_7bit::UNICODE_MAP_SF7DEC))),
            202 => Ok(Box::new(CharSet7Bit::new("E7SIEMENS9780X", charset_7bit::UNICODE_MAP_E7SIEMENS9780X))),
            203 => Ok(Box::new(CharSet7Bit::new("S7SIEMENS9780X", charset_7bit::UNICODE_MAP_S7SIEMENS9780X))),
            204 => Ok(Box::new(CharSet7Bit::new("DK7SIEMENS9780X", charset_7bit::UNICODE_MAP_DK7SIEMENS9780X))),
            205 => Ok(Box::new(CharSet7Bit::new("N7SIEMENS9780X", charset_7bit::UNICODE_MAP_N7SIEMENS9780X))),
            206 => Ok(Box::new(CharSet7Bit::new("I7SIEMENS9780X", charset_7bit::UNICODE_MAP_I7SIEMENS9780X))),
            207 => Ok(Box::new(CharSet7Bit::new("D7SIEMENS9780X", charset_7bit::UNICODE_MAP_D7SIEMENS9780X))),
            _ => olr_err!(010003, "Unknown charset: {}", charset_id).into()
        }
    }
}


