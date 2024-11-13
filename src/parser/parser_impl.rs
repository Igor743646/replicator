use std::fmt::Display;
use std::io::{self, Seek};
use std::num::NonZeroU32;
use std::{fs::File, io::Read, path::PathBuf};

use bytebuffer::Endian::*;
use clap::error::Result;
use log::{debug, warn};

use crate::common::types::{TypeRBA, TypeTimestamp};
use crate::olr_perr;
use crate::{common::{errors::OLRError, types::TypeSeq}, olr_err};
use crate::common::errors::OLRErrorCode::*;

use super::byte_reader_ws::ByteReaderWithSkip;

#[derive(Debug, Default)]
pub struct BlockHeader {
    pub block_flag  : u8,
    pub file_type   : u8,
    pub rba         : TypeRBA,
    pub checksum    : u16,
}

impl Display for BlockHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Block header: 0x{:02X}{:02X} RBA: {}, Checksum: 0x{:04X}", self.block_flag, self.file_type, self.rba, self.checksum)
    }
}

#[derive(Debug, Default)]
pub struct RedoLogHeader {
    pub block_header : BlockHeader,
    pub oracle_version : u32,
    pub database_id : u32,
    pub database_name : String,
    pub control_sequence : u32,
    pub file_size : u32,
    pub file_number : u16,
    pub activation_id : u32,
    pub description : String,
    pub blocks_count : u32,
    pub resetlogs_id : TypeTimestamp,
}

#[derive(Debug, Eq, Default)]
pub struct Parser {
    file_path : PathBuf,
    sequence : TypeSeq,

    block_size : Option<NonZeroU32>,
    endian     : Option<bytebuffer::Endian>,
}

impl PartialEq for Parser {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl PartialOrd for Parser {
    fn lt(&self, other: &Self) -> bool {
        self.sequence < other.sequence
    }

    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sequence.partial_cmp(&other.sequence)
    }
}

impl Ord for Parser {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sequence.cmp(&other.sequence)
    }
}

impl Parser {
    pub fn new(file_path : PathBuf, sequence : TypeSeq) -> Self {
        Self { file_path, sequence, .. Default::default() }
    }

    pub fn sequence(&self) -> TypeSeq {
        self.sequence
    }

    pub fn parse(&mut self) -> Result<(), OLRError> {
        
        let mut archive_log = File::open(&self.file_path)
                .or(olr_err!(FileReading, "Can not open archive file"))?;

        let metadata = archive_log.metadata().unwrap();

        if metadata.len() % 512 != 0 {
            warn!("The file size is not a multiple of 512. File size: {}", metadata.len());
        }


        self.check_file_header(&mut archive_log)?;

        let block_size = self.block_size.unwrap().get() as usize;
        archive_log.seek(io::SeekFrom::Start(block_size as u64)).unwrap();
        let boxed_buffer = Box::<[u8]>::new_uninit_slice(block_size);
        let mut buffer = unsafe { boxed_buffer.assume_init() };
        let mut read_buffer = buffer.as_mut();

        let redo_log_header = self.get_redo_log_info(&mut read_buffer, &mut archive_log)?;

        while archive_log.read(read_buffer).unwrap() == block_size {
            let reader = ByteReaderWithSkip::from_bytes(&mut read_buffer);
            
            debug!("{}", reader.to_hex_dump());
            break;
        }
        
        Ok(())
    }

    fn check_file_header(&mut self, archive_log : &mut File) -> Result<(), OLRError> {
        let mut read_buffer = [0u8; 512];
        archive_log.read_exact(&mut read_buffer)
            .or(olr_perr!("Can not read first 512 bytes"))?;
        let mut reader = ByteReaderWithSkip::from_bytes(&mut read_buffer);

        if let Some(endian) = self.endian {
            reader.set_endian(endian);
        } else {
            self.endian = Some(LittleEndian);
            reader.set_endian(LittleEndian);
            reader.skip_bytes(28);
            let magic_number = reader.read_u32().unwrap();
            match magic_number {
                0x7A7B7C7D => {},
                0x7D7C7B7A => { self.endian = Some(BigEndian); },
                _ => {
                    return olr_perr!("Unknown magic number in file header. {}", reader.to_error_hex_dump(28, 4));
                },
            }
            reader.reset_cursors();
        }

        let block_flag = reader.read_u8().unwrap();
        let file_type = reader.read_u8().unwrap();
        reader.skip_bytes(18);
        let block_size = reader.read_u32().unwrap();
        let number_of_blocks = reader.read_u32().unwrap();
        // let magic_number = reader.read_u32().unwrap();

        if block_flag != 0 {
            return olr_perr!("Invalid block flag: {}. {}", block_flag, reader.to_error_hex_dump(0, 1));
        }

        if (file_type == 0x22 && (block_size == 512 || block_size == 1024)) || (file_type == 0x82 && block_size == 4096) {
            self.block_size = Some(NonZeroU32::new(block_size).unwrap());
        } else {
            return olr_perr!("Invalid block size: {}. {}", block_size, reader.to_error_hex_dump(20, 4));
        }

        let file_size = archive_log.metadata().unwrap().len();

        if file_size != ((number_of_blocks + 1) * block_size) as u64 {
            return olr_perr!("Invalid file size. ({} + 1) * {} != {} bytes. {}", number_of_blocks, block_size, file_size, reader.to_error_hex_dump(24, 4));
        }
        
        Ok(())
    }

    fn get_redo_log_info(&self, read_buffer : &mut [u8], archive_log : &mut File) -> Result<RedoLogHeader, OLRError> {
        archive_log.read_exact(read_buffer)
            .or(olr_perr!("Can not read redo log header"))?;
        let mut reader = ByteReaderWithSkip::from_bytes(read_buffer);
        reader.set_endian(self.endian.unwrap());

        self.validate_block(read_buffer)?;

        let mut redo_log_header : RedoLogHeader = RedoLogHeader::default();

        redo_log_header.block_header = reader.read_block_header().unwrap();
        reader.skip_bytes(4);
        redo_log_header.oracle_version = reader.read_u32().unwrap();
        redo_log_header.database_id = reader.read_u32().unwrap();
        redo_log_header.database_name = String::from_utf8(reader.read_bytes(8).unwrap()).unwrap();
        redo_log_header.control_sequence = reader.read_u32().unwrap();
        redo_log_header.file_size = reader.read_u32().unwrap();
        reader.skip_bytes(4);
        redo_log_header.file_number = reader.read_u16().unwrap();
        reader.skip_bytes(2);
        redo_log_header.activation_id = reader.read_u32().unwrap();
        reader.skip_bytes(36);
        redo_log_header.description = String::from_utf8(reader.read_bytes(64).unwrap()).unwrap();
        redo_log_header.blocks_count = reader.read_u32().unwrap();
        redo_log_header.resetlogs_id = reader.read_u32().unwrap().into();

        debug!("{:#?}", redo_log_header);
        debug!("{}", reader.to_hex_dump());




        Ok(redo_log_header)
    }

    fn validate_block(&self, read_buffer : &[u8]) -> Result<(), OLRError> {
        let checksum = self.block_checksum(read_buffer);
        if checksum != 0 {
            let mut reader = ByteReaderWithSkip::from_bytes(read_buffer);
            reader.skip_bytes(14);
            let block_checksum = reader.read_u16().unwrap();
            return olr_perr!("Bad block. Checksums are not equal: {} != {}. {}", checksum ^ block_checksum, block_checksum, reader.to_error_hex_dump(14, 2));
        }
        Ok(())
    }

    fn block_checksum(&self, read_buffer : &[u8]) -> u16 {
        let block_size: usize = self.block_size.unwrap().get() as usize;
        let mut reader = ByteReaderWithSkip::from_bytes(read_buffer);
        reader.set_endian(self.endian.unwrap());

        debug_assert!(read_buffer.len() >= block_size);

        let mut checksum: u64 = 0;

        for _ in 0 .. block_size / 8 {
            let chunk = reader.read_u64().unwrap();
            checksum ^= chunk;
        }

        checksum = (checksum >> 32) ^ checksum;
        checksum = (checksum >> 16) ^ checksum;

        (checksum & 0xFFFF) as u16
    }
}


