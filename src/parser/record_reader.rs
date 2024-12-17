use super::{archive_structs::vector_header::VectorHeader, byte_reader::ByteReader};

#[derive(Debug)]
pub struct VectorReader<'a> {
    pub header : VectorHeader,
    data : &'a [u8],
    current_pos : usize,
    current_field : usize,
}

impl<'a> VectorReader<'a> {
    pub fn new(vector_header : VectorHeader, vector_data : &'a [u8]) -> Self {
        Self {
            header : vector_header,
            data : vector_data,
            current_pos : 0,
            current_field : 0,
        }
    }

    #[allow(unused)]
    pub fn reset(&mut self) {
        self.current_pos = 0;
        self.current_field = 0;
    }

    pub fn eof(&self) -> bool {
        self.current_field >= self.header.fields_count as usize
    }

    pub fn get_field_nth(&self, n : usize) -> ByteReader {
        let mut fsize = self.header.fields_sizes[0] as usize;
        let mut pos = 0;
        for i in 0 .. n {
            pos += (fsize  + 3) & !3;
            fsize = self.header.fields_sizes[i + 1] as usize;
        }
        ByteReader::from_bytes(&self.data[pos .. pos + fsize])
    }

    pub fn skip_empty(&mut self) -> usize {
        let mut result = 0;

        while !self.eof() && self.header.fields_sizes[self.current_field] == 0 {
            let _ = self.next();
            result += 1;
        }

        result
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        let tail_size = self.header.fields_count as usize - self.current_field;
        (tail_size, Some(tail_size))
    }
}
