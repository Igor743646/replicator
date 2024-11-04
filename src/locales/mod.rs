use std::collections::HashMap;

use charset::CharacterDecoder;
use charset_7bit::CharSet7Bit;

mod charset;
mod charset_7bit;

#[derive(Default)]
pub struct Locales {
    pub character_map   : HashMap<u64, Box<dyn CharacterDecoder>>,
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

        result.character_map.insert(1, Box::new(CharSet7Bit::new("US7ASCII", charset_7bit::UNICODE_MAP_US7_ASCII)));

        result
    }
}
