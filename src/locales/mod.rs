use std::collections::HashMap;

pub mod charset;
pub mod charset_7bit;

use charset::CharacterMapper;
use charset_7bit::CharSet7Bit;


#[derive(Default)]
pub struct Locales {
    pub character_map   : HashMap<u64, Box<dyn CharacterMapper>>,
    pub timezone_map    : HashMap<u16, &'static str>,
}

impl std::fmt::Debug for Locales {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Locales")
    }
}

impl Locales {
    pub fn new() -> Self {
        let mut result = Self::default();

        // 7-bit charsets
        result.character_map.insert(1, Box::new(CharSet7Bit::new("US7ASCII", charset_7bit::UNICODE_MAP_US7ASCII)));
        result.character_map.insert(11, Box::new(CharSet7Bit::new("D7DEC", charset_7bit::UNICODE_MAP_D7DEC)));
        result.character_map.insert(13, Box::new(CharSet7Bit::new("S7DEC", charset_7bit::UNICODE_MAP_S7DEC)));
        result.character_map.insert(14, Box::new(CharSet7Bit::new("E7DEC", charset_7bit::UNICODE_MAP_E7DEC)));
        result.character_map.insert(15, Box::new(CharSet7Bit::new("SF7ASCII", charset_7bit::UNICODE_MAP_SF7ASCII)));
        result.character_map.insert(16, Box::new(CharSet7Bit::new("NDK7DEC", charset_7bit::UNICODE_MAP_NDK7DEC)));
        result.character_map.insert(17, Box::new(CharSet7Bit::new("I7DEC", charset_7bit::UNICODE_MAP_I7DEC)));
        result.character_map.insert(21, Box::new(CharSet7Bit::new("SF7DEC", charset_7bit::UNICODE_MAP_SF7DEC)));
        result.character_map.insert(202, Box::new(CharSet7Bit::new("E7SIEMENS9780X", charset_7bit::UNICODE_MAP_E7SIEMENS9780X)));
        result.character_map.insert(203, Box::new(CharSet7Bit::new("S7SIEMENS9780X", charset_7bit::UNICODE_MAP_S7SIEMENS9780X)));
        result.character_map.insert(204, Box::new(CharSet7Bit::new("DK7SIEMENS9780X", charset_7bit::UNICODE_MAP_DK7SIEMENS9780X)));
        result.character_map.insert(205, Box::new(CharSet7Bit::new("N7SIEMENS9780X", charset_7bit::UNICODE_MAP_N7SIEMENS9780X)));
        result.character_map.insert(206, Box::new(CharSet7Bit::new("I7SIEMENS9780X", charset_7bit::UNICODE_MAP_I7SIEMENS9780X)));
        result.character_map.insert(207, Box::new(CharSet7Bit::new("D7SIEMENS9780X", charset_7bit::UNICODE_MAP_D7SIEMENS9780X)));

        result
    }
}
