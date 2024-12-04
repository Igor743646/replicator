use super::{byte_reader::ByteReader, parser_impl::RedoVectorHeader};

#[derive(Debug)]
pub struct VectorReader<'a> {
    pub header : RedoVectorHeader,
    data : &'a [u8],
    current_pos : usize,
    current_field : usize,
}

impl<'a> VectorReader<'a> {
    pub fn new(vector_header : RedoVectorHeader, vector_data : &'a [u8]) -> Self {
        Self {
            header : vector_header,
            data : vector_data,
            current_pos : 0,
            current_field : 0,
        }
    }

    pub fn reset(&mut self) {
        self.current_pos = 0;
        self.current_field = 0;
    }

    pub fn eof(&self) -> bool {
        self.current_field >= self.header.fields_count as usize
    }
}

impl<'a> Iterator for VectorReader<'a> {
    type Item = ByteReader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_field >= self.header.fields_count as usize {
            None
        } else {
            let field_size = self.header.fields_sizes[self.current_field] as usize;
            let reader = ByteReader::from_bytes(&self.data[self.current_pos .. self.current_pos + field_size]);
            self.current_pos += (field_size  + 3) & !3;
            self.current_field += 1;
            Some(reader)
        }
    }
}
