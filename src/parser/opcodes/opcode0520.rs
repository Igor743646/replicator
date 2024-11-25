use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}}};

#[derive(Default)]
pub struct OpCode0520 {
    pub session_number : u32,
    pub serial_number : u16,
    pub version : u32,
    pub audit_session_id : u32,
    pub login_username : String,
}

impl OpCode0520 {
    pub fn session_attribute_1(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 4, "Size of field {} < 4", vector_header.fields_sizes[field_num]);

        if parser.version().unwrap() < constants::REDO_VERSION_19_0 {
            self.session_number = reader.read_u16()? as u32;
            self.serial_number = reader.read_u16()?;

            reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 4);
        } else {
            assert!(vector_header.fields_sizes[field_num] >= 8, "Size of field {} < 8", vector_header.fields_sizes[field_num]);
            reader.skip_bytes(2);
            self.serial_number = reader.read_u16()?;
            self.session_number = reader.read_u32()?;

            reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 8);
        }

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] session number: {} serial number: {}", field_num, self.session_number, self.serial_number));
        }

        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_2(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        
        if parser.can_dump(1) {
            let data = reader.read_bytes(vector_header.fields_sizes[field_num] as usize)?;
            parser.write_dump(format_args!("\n[Change {}] unknown attribute: {}", field_num, String::from_utf8(data).unwrap_or_default()));
        } else {
            reader.skip_bytes(vector_header.fields_sizes[field_num] as usize);
        }

        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_3(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 6, "Size of field {} < 6", vector_header.fields_sizes[field_num]);

        if parser.can_dump(1) {
            let flg1 = reader.read_u16()?;
            reader.skip_bytes(2);
            let flg2 = reader.read_u16()?;
            parser.write_dump(format_args!("\n[Change {}] Flg 1: {} Flg 2: {}", field_num, flg1, flg2));

            reader.skip_bytes(vector_header.fields_sizes[field_num] as usize - 6);
        } else {
            reader.skip_bytes(vector_header.fields_sizes[field_num] as usize);
        }

        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_4(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 4, "Size of field {} < 4", vector_header.fields_sizes[field_num]);

        self.version = reader.read_u32()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Version: {}", field_num, self.version));
        }

        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_5(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(vector_header.fields_sizes[field_num] >= 4, "Size of field {} < 4", vector_header.fields_sizes[field_num]);

        self.audit_session_id = reader.read_u32()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Audit session id: {}", field_num, self.audit_session_id));
        }

        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_6(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize);
        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_7(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        reader.skip_bytes(vector_header.fields_sizes[field_num] as usize);
        reader.align_up(4);
        Ok(())
    }

    pub fn session_attribute_8(&mut self, parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        
        self.login_username = String::from_utf8(reader.read_bytes(vector_header.fields_sizes[field_num] as usize)?).unwrap_or_default();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Login username: {}\n", field_num, self.login_username));
        }

        reader.align_up(4);
        Ok(())
    }
}

impl VectorParser for OpCode0520 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut ByteReader) -> Result<(), OLRError> {
        assert!(vector_header.fields_count <= 8, "Count of fields ({}) > 8", vector_header.fields_count);
        let mut result = OpCode0520::default();

        result.session_attribute_1(parser, vector_header, reader, 0)?;

        if vector_header.fields_count >= 2 {
            result.session_attribute_2(parser, vector_header, reader, 1)?;
        }

        if vector_header.fields_count >= 3 {
            result.session_attribute_3(parser, vector_header, reader, 2)?;
        }

        if vector_header.fields_count >= 4 {
            result.session_attribute_4(parser, vector_header, reader, 3)?;
        }

        if vector_header.fields_count >= 5 {
            result.session_attribute_5(parser, vector_header, reader, 4)?;
        }

        if vector_header.fields_count >= 6 {
            result.session_attribute_6(parser, vector_header, reader, 5)?;
        }

        if vector_header.fields_count >= 7 {
            result.session_attribute_7(parser, vector_header, reader, 6)?;
        }

        if vector_header.fields_count >= 8 {
            result.session_attribute_8(parser, vector_header, reader, 7)?;
        }
        
        Ok(())
    }
}
