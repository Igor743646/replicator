use crate::{common::{constants, errors::OLRError}, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

use super::VectorField;


pub struct Ktub {
    pub obj : u32,
    pub data_obj : u32,
    pub opc : (u8, u8),
    pub slt : u16,
    pub flg : u16,
}

impl VectorField for Ktub {
    fn parse_from_reader(parser : &mut Parser, _vec_reader : &mut VectorReader, reader : &mut ByteReader, field_num : usize) -> Result<Self, OLRError> {
        assert!(reader.data().len() >= 24, "Size of field {} < 24", reader.data().len());

        let obj        = reader.read_u32()?;
        let data_obj   = reader.read_u32()?;
        reader.skip_bytes(4);
        let _undo  = reader.read_u32()?;
        let opc0      = reader.read_u8()?;
        let opc1      = reader.read_u8()?;
        let slt        = reader.read_u8()? as u16;
        reader.skip_bytes(1);
        let flg        = reader.read_u16()?;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUBL - {}] OBJ: {} DATAOBJ: {}\nOPC: {}.{} SLT: {}\nFLG: {:016b}\n", 
                    field_num, reader.data().len(), obj, data_obj, opc0, opc1, slt, flg))?;

            let tbl = ["NO", "YES"];
            parser.write_dump(format_args!(" MULTI BLOCK UNDO HEAD : {:>3}\n", tbl[(flg & constants::FLG_MULTIBLOCKUNDOHEAD != 0) as usize]))?;
            parser.write_dump(format_args!(" MULTI BLOCK UNDO TAIL : {:>3}\n", tbl[(flg & constants::FLG_MULTIBLOCKUNDOTAIL != 0) as usize]))?;
            parser.write_dump(format_args!(" LAST BUFFER SPLIT     : {:>3}\n", tbl[(flg & constants::FLG_LASTBUFFERSPLIT    != 0) as usize]))?;
            parser.write_dump(format_args!(" BEGIN TRANSACTION     : {:>3}\n", tbl[(flg & constants::FLG_BEGIN_TRANS        != 0) as usize]))?;
            parser.write_dump(format_args!(" USER UNDO DONE        : {:>3}\n", tbl[(flg & constants::FLG_USERUNDODDONE      != 0) as usize]))?;
            parser.write_dump(format_args!(" IS TEMPORARY OBJECT   : {:>3}\n", tbl[(flg & constants::FLG_ISTEMPOBJECT       != 0) as usize]))?;
            parser.write_dump(format_args!(" USER ONLY             : {:>3}\n", tbl[(flg & constants::FLG_USERONLY           != 0) as usize]))?;
            parser.write_dump(format_args!(" TABLESPACE UNDO       : {:>3}\n", tbl[(flg & constants::FLG_TABLESPACEUNDO     != 0) as usize]))?;
            parser.write_dump(format_args!(" MULTI BLOCK UNDO MID  : {:>3}\n", tbl[(flg & constants::FLG_MULTIBLOCKUNDOMID  != 0) as usize]))?;
        }

        Ok(Ktub {obj, data_obj, opc : (opc0, opc1), slt, flg} )
    }
}
