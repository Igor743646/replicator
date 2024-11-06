
pub trait CharacterDecoder {
    fn decode(&self, string : &[u8]) -> Vec<u8>;
}

