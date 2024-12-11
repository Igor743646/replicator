use super::{VectorInfo, VectorParser};
use crate::{common::{constants, errors::OLRError, types::TypeXid}, olr_perr, parser::{byte_reader::ByteReader, parser_impl::Parser, record_reader::VectorReader}};

#[derive(Debug)]
pub struct OpCode0502<'a> {
    pub xid : TypeXid,
    pub flg : u16,

    reader : VectorReader<'a>,
}

impl<'a> OpCode0502<'a> {
    pub fn new(parser : &mut Parser, reader : VectorReader<'a>) -> Result<Self, OLRError> {
        let mut res = Self {
            xid : Default::default(),
            flg : Default::default(),
            reader,
        };
        res.init(parser)?;
        Ok(res)
    }

    fn init(&mut self, parser : &mut Parser) -> Result<(), OLRError> {
        if self.reader.header.fields_count > 3 {
            return olr_perr!("Opcode: 5.2 Count of field > 3. Dump: {}", self.reader.by_ref().map(|x| {x.to_hex_dump()}).collect::<String>());
        }

        match self.reader.next() {
            Some(mut field_reader) => self.ktudh(parser, &mut field_reader, 0),
            None => olr_perr!("Expect ktudh field")
        }?;

        if parser.version().unwrap() >= constants::REDO_VERSION_12_1 && parser.can_dump(1) {
            if let Some(mut field_reader) = self.reader.next() {
                if field_reader.data().len() == 4 {
                    self.pdb(parser, &mut field_reader, 1)?;
                } else {
                    self.kteop(parser, &mut field_reader, 1)?;

                    if let Some(mut field_reader) = self.reader.next() {
                        self.pdb(parser, &mut field_reader, 2)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn ktudh(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 32, "Size of field {} != 32", reader.data().len());

        let xid_usn = (self.reader.header.class - 15) / 2;
        let xid_slot = reader.read_u16()?;
        reader.skip_bytes(2);
        let xid_seq = reader.read_u32()?;
        reader.skip_bytes(8);
        let flg = reader.read_u16()?;
        reader.skip_bytes(14);

        self.xid = (((xid_usn as u64) << 48) | ((xid_slot as u64) << 32) | xid_seq as u64).into();
        self.flg = flg;

        if parser.can_dump(1) {
            parser.write_dump(format_args!("\n[Change {}; KTUDH] XID: {}\nFlag: {:016b}\n", field_num, self.xid, self.flg))?;
        }

        Ok(())
    }

    fn pdb(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 4, "Size of field {} != 4", reader.data().len());

        if parser.can_dump(1) {
            let pdb_id = reader.read_u32()?;
            parser.write_dump(format_args!("\n[Change {}; PDB] PDB id: {}\n", field_num, pdb_id))?;
        }
        
        Ok(())
    }

    fn kteop(&mut self, parser : &mut Parser, reader : &mut ByteReader, field_num : usize) -> Result<(), OLRError> {
        assert!(reader.data().len() == 36, "Size of field {} != 36", reader.data().len());

        if parser.can_dump(1) {
            reader.skip_bytes(4);
            let ext = reader.read_u32()?;
            reader.skip_bytes(4);
            let ext_size = reader.read_u32()?;
            let highwater = reader.read_u32()?;
            reader.skip_bytes(4);
            let offset = reader.read_u32()?;

            parser.write_dump(format_args!("\n[Change {}; KTEOP] ext: {} ext size: {} HW: {} offset: {}\n", field_num, ext, ext_size, highwater, offset))?;
        }

        Ok(())
    }
}

impl<'a> VectorParser<'a> for OpCode0502<'a> {
    fn parse(parser : &mut Parser, reader : VectorReader<'a>) -> Result<VectorInfo<'a>, OLRError> {
        Ok(
            VectorInfo::OpCode0502(
                OpCode0502::new(parser, reader)?
            )
        )
    }
}
