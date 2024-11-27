use super::VectorParser;
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::{Parser, RedoVectorHeader}, record_reader::VectorReader}};

#[derive(Default)]
pub struct OpCode0520 {
    pub session_number : u32,
    pub serial_number : u16,
    pub version : u32,
    pub audit_session_id : u32,
    pub login_username : String,
}

impl OpCode0520 {
    pub fn session_attribute_1(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 4, "Size of field {} < 4", reader.data().len());

        if parser.version().unwrap() < constants::REDO_VERSION_19_0 {
            self.session_number = reader.read_u16()? as u32;
            self.serial_number = reader.read_u16()?;

            reader.skip_bytes(reader.data().len() - 4);
        } else {
            assert!(reader.data().len() >= 8, "Size of field {} < 8", reader.data().len());
            reader.skip_bytes(2);
            self.serial_number = reader.read_u16()?;
            self.session_number = reader.read_u32()?;

            reader.skip_bytes(reader.data().len() - 8);
        }

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] session number: {} serial number: {}", field_num, self.session_number, self.serial_number));
        }

        Ok(())
    }

    pub fn session_attribute_2(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        
        if parser.can_dump(1) {
            let data = reader.read_bytes(reader.data().len() as usize)?;
            parser.write_dump(format_args!("\n[Change {}] unknown attribute: {}", field_num, String::from_utf8(data).unwrap_or_default()));
        } else {
            reader.skip_bytes(reader.data().len() as usize);
        }

        Ok(())
    }

    pub fn session_attribute_3(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 6, "Size of field {} < 6", reader.data().len());

        if parser.can_dump(1) {
            let flg1 = reader.read_u16()?;
            reader.skip_bytes(2);
            let flg2 = reader.read_u16()?;
            parser.write_dump(format_args!("\n[Change {}] Flg 1: {} Flg 2: {}", field_num, flg1, flg2));

            reader.skip_bytes(reader.data().len() as usize - 6);
        } else {
            reader.skip_bytes(reader.data().len() as usize);
        }

        Ok(())
    }

    pub fn session_attribute_4(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 4, "Size of field {} < 4", reader.data().len());

        self.version = reader.read_u32()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Version: {}", field_num, self.version));
        }

        Ok(())
    }

    pub fn session_attribute_5(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() >= 4, "Size of field {} < 4", reader.data().len());

        self.audit_session_id = reader.read_u32()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Audit session id: {}", field_num, self.audit_session_id));
        }

        Ok(())
    }

    pub fn session_attribute_6(&mut self, _ : &mut Parser, _ : &mut ByteReader, _ : usize) -> Result<(), OLRError> {
        Ok(())
    }

    pub fn session_attribute_7(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        if parser.can_dump(1) {
            let client_id = String::from_utf8(reader.read_bytes(reader.data().len() as usize)?).unwrap_or_default();
            parser.write_dump(format_args!("\n[Change {}] Client id: {}", field_num, client_id));
        }
        Ok(())
    }

    pub fn session_attribute_8(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        self.login_username = String::from_utf8(reader.read_bytes(reader.data().len() as usize)?).unwrap_or_default();

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}] Login username: {}\n", field_num, self.login_username));
        }

        Ok(())
    }
}

impl VectorParser for OpCode0520 {
    fn parse(parser : &mut Parser, vector_header: &RedoVectorHeader, reader : &mut VectorReader) -> Result<(), OLRError> {
        let mut result = OpCode0520::default();

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_1(parser, &mut field_reader, 0)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_2(parser, &mut field_reader, 1)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_3(parser, &mut field_reader, 2)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_4(parser, &mut field_reader, 3)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_5(parser, &mut field_reader, 4)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_6(parser, &mut field_reader, 5)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_7(parser, &mut field_reader, 6)?;
        }

        if let Some(mut field_reader) = reader.next_field_reader() {
            result.session_attribute_8(parser, &mut field_reader, 7)?;
        }
        
        Ok(())
    }
}
