
pub trait CharacterMapper {
    fn map_to_utf8(&self, string : &[u8]) -> Vec<u8>;
}

