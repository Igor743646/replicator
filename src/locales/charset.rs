
pub trait CharacterDecoder {
    fn decode(&self, string : &mut [u8], length: &mut usize) -> u64;
}

