use core::fmt;
use std::fmt::Display;
use std::fs::{File, Metadata};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use std::path::PathBuf;

use clap::error::Result;
use log::info;

use crate::common::thread::spawn;
use crate::common::types::{TypeRBA, TypeRecordScn, TypeScn, TypeTimestamp};
use crate::ctx::Ctx;
use crate::olr_perr;
use crate::parser::fs_reader::{Reader, ReaderMessage};
use crate::parser::record_analizer::RecordAnalizer;
use crate::parser::records_manager::Record;
use crate::{common::{errors::OLRError, types::TypeSeq}, olr_err};
use crate::common::errors::OLRErrorCode::*;

use super::byte_reader::{self, ByteReader};
use super::records_manager::RecordsManager;

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
    pub resetlogs_scn : TypeScn,
    pub hws : u32,
    pub thread : u16,
    pub first_scn : TypeScn,
    pub first_time : TypeTimestamp,
    pub next_scn : TypeScn,
    pub next_time : TypeTimestamp,
    pub eot : u8,
    pub dis : u8,
    pub zero_blocks : u8,
    pub format_id : u8,
    pub enabled_scn : TypeScn,
    pub enabled_time : TypeTimestamp,
    pub thread_closed_scn : TypeScn,
    pub thread_closed_time : TypeTimestamp,
    pub misc_flags : u32,
    pub terminal_recovery_scn : TypeScn,
    pub terminal_recovery_time : TypeTimestamp,
    pub most_recent_scn : TypeScn,
    pub largest_lwn : u32,
    pub real_next_scn : TypeScn,
    pub standby_apply_delay : u32,
    pub prev_resetlogs_scn : TypeScn,
    pub prev_resetlogs_id : TypeTimestamp,
    pub misc_flags_2 : u32,
    pub standby_log_close_time : TypeTimestamp,
    pub thr : i32,
    pub seq2 : i32,
    pub scn2 : TypeScn,
    pub redo_log_key : [u8; 16],
    pub redo_log_key_flag : u16,
}

#[derive(Debug, Default)]
pub struct RedoRecordHeaderExpansion {
    pub record_num          : u16,
    pub record_num_max      : u16,
    pub records_count       : u32,
    pub records_scn         : TypeScn,
    pub scn1                : TypeScn,
    pub scn2                : TypeScn,
    pub records_timestamp   : TypeTimestamp,
}

#[derive(Debug, Default)]
pub struct RedoRecordHeader {
    pub record_size : u32,
    pub vld : u8,
    pub scn : TypeRecordScn,
    pub sub_scn : u16,
    pub container_uid : Option<u32>,
    pub expansion : Option<RedoRecordHeaderExpansion>,
}

#[derive(Debug)]
pub struct Parser {
    pub context_ptr : Arc<Ctx>,
    file_path : PathBuf,
    sequence : TypeSeq,

    block_size : Option<usize>,
    endian     : Option<byte_reader::Endian>,
    metadata   : Option<Metadata>,
    pub dump_file  : Option<File>,

    records_manager : RecordsManager,
}

impl PartialEq for Parser {
    fn eq(&self, other: &Self) -> bool {
        self.sequence.eq(&other.sequence)
    }
}

impl Eq for Parser {}

impl PartialOrd for Parser {
    fn lt(&self, other: &Self) -> bool {
        self.sequence.lt(&other.sequence)
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
    pub fn new(context_ptr : Arc<Ctx> , file_path : PathBuf, sequence : TypeSeq) -> Self {
        let mut dump_file = None;
        if context_ptr.dump.level > 0 {
            std::fs::create_dir_all(PathBuf::new().join(context_ptr.dump.path.as_str())).unwrap();
            let dump_path = PathBuf::new().join(context_ptr.dump.path.as_str()).join(format!("dump-{}.ansi", sequence));
            dump_file = Some(File::create(dump_path).unwrap());
        }
        Self {
            context_ptr: context_ptr.clone(), 
            file_path, 
            sequence,
            block_size : None, 
            endian : None, 
            metadata : None,
            dump_file,
            records_manager : RecordsManager::new(context_ptr.clone()),
        }
    }

    pub fn write_dump(&mut self, fmt: fmt::Arguments<'_>) {
        if let Some(file) = &mut self.dump_file {
            file.write_fmt(fmt).unwrap();
        }
    }

    pub fn sequence(&self) -> TypeSeq {
        self.sequence
    }

    pub fn parse(&mut self) -> Result<(), OLRError> {

        let start_parsing_time = Instant::now();

        let (sx, rx) = self.context_ptr.get_reader_channel();
        let fs_reader = Reader::new(self.context_ptr.clone(), self.file_path.clone(), sx);
        let fs_reader_handle = spawn(fs_reader)?;

        let message = rx.recv().unwrap();

        match message {
            ReaderMessage::Start(block_size, metadata, endian) => {
                self.block_size = block_size.into();
                self.endian = endian.into();
                self.metadata = metadata.into();
            },
            data => return olr_err!(ChannelSend, "Wrong data in first message: {:?}", data),
        }

        let mut to_read : usize = 0;
        let mut start_block : usize = 0;
        let mut end_block : usize = 0;
        let mut redo_log_header : RedoLogHeader = Default::default();
        let mut record_position = 0;
        let mut record : Option<&mut Record> = None;

        loop {
            let message = rx.recv().unwrap();

            let (chunk, blocks_count) = match message {
                ReaderMessage::Read(chunk, size) => {
                    assert!(size > 0);
                    (chunk, size / self.block_size.unwrap())
                },
                ReaderMessage::Eof => break,
                _ => return olr_err!(ChannelSend, "Unexpected message type: {:?}", message),
            };

            for idx in 0 .. blocks_count {
                let range = idx * self.block_size.unwrap() .. (idx + 1) * self.block_size.unwrap();
                let phisical_block = &chunk[range];

                if start_block == 0 {
                    self.check_file_header(&phisical_block)?;
                    start_block += 1;
                    end_block += 1;
                    continue;
                }

                if start_block == 1 {
                    redo_log_header = self.get_redo_log_header(&phisical_block)?;
                    self.write_dump(format_args!("{:#?}", redo_log_header));
                    start_block += 1;
                    end_block += 1;
                    continue;
                }

                let mut reader = ByteReader::from_bytes(&phisical_block);
                reader.set_endian(self.endian.unwrap());

                reader.skip_bytes(16); // Skip block header
                
                if start_block == end_block {
                    let redo_record_header = match reader.read_redo_record_header(redo_log_header.oracle_version) {
                        Ok(x) => x,
                        Err(err) => return olr_perr!("Parse record header error: {}. {}", err, reader.to_error_hex_dump(16, 68))
                    };

                    assert!(redo_record_header.expansion.is_some(), "Dump: {}", reader.to_error_hex_dump(16, 68));
                    end_block = start_block + redo_record_header.expansion.unwrap().records_count as usize;

                    reader.reset_cursor();
                    reader.skip_bytes(16);
                }

                while reader.cursor() < self.block_size.unwrap() {
                    if to_read == 0 {
                        if reader.cursor() + 20 >= self.block_size.unwrap() {
                            break;
                        }

                        let prev_offset = reader.cursor();
                        let redo_record_header = match reader.read_redo_record_header(redo_log_header.oracle_version) {
                            Ok(x) => x,
                            Err(err) => return olr_perr!("Parse record header error: {}. {}", err, reader.to_error_hex_dump(16, 24))
                        };

                        if redo_record_header.record_size == 0 {
                            break;
                        }

                        to_read = redo_record_header.record_size as usize;
                        record_position = 0;

                        record = Some(self.records_manager.reserve_record(to_read)?);
                        record.as_mut().unwrap().scn = redo_record_header.scn;
                        record.as_mut().unwrap().sub_scn = redo_record_header.sub_scn;
                        record.as_mut().unwrap().block = start_block as u32;
                        record.as_mut().unwrap().offset = prev_offset as u16;
                        record.as_mut().unwrap().size = redo_record_header.record_size;
                        
                        reader.reset_cursor();
                        reader.skip_bytes(prev_offset);
                    }

                    let to_copy = std::cmp::min(to_read, self.block_size.unwrap() - reader.cursor());

                    let buffer = &mut record.as_mut().unwrap().data()[record_position .. record_position + to_copy];

                    assert!(buffer.len() == to_copy);
                    reader.read_bytes_into(to_copy, buffer).unwrap();
                    
                    to_read -= to_copy;
                    record_position += to_copy;
                }

                if start_block + 1 == end_block {
                    // Process data here

                    while self.records_manager.records_count() > 0 {
                        let record = self.records_manager.drop_record();

                        if record.is_none() {
                            return olr_perr!("No record, but expected");
                        }

                        self.analize_record(record.unwrap())?;
                    }

                    self.records_manager.free_chunks();
                }
                start_block += 1;
            }

            {
                self.context_ptr.free_chunk(chunk);
            }
        }

        assert!(self.records_manager.records_count() == 0);

        info!("Time elapsed: {:?}", start_parsing_time.elapsed());
        fs_reader_handle.join().unwrap()?;
        Ok(())
    }

    fn check_file_header(&self, buffer : &[u8]) -> Result<(), OLRError> {
        let mut reader = ByteReader::from_bytes(&buffer);

        assert!(self.block_size.is_some());
        assert!(self.endian.is_some());

        let block_flag = reader.read_u8().unwrap();
        let file_type = reader.read_u8().unwrap();
        reader.skip_bytes(18);
        let block_size = reader.read_u32().unwrap();
        let number_of_blocks = reader.read_u32().unwrap();
        let magic_number = reader.read_u32().unwrap();

        if block_flag != 0 {
            return olr_perr!("Invalid block flag: {}, expected 0x00. {}", block_flag, reader.to_error_hex_dump(0, 1));
        }

        match (file_type, block_size) {
            (0x22, 512) | (0x22, 1024) | (0x82, 4096) => {
                assert_eq!(self.block_size.unwrap(), block_size as usize);
            },
            _ => {
                return olr_perr!("Invalid block size: {}, expected one of {{512, 1024, 4096}}. {}", block_size, reader.to_error_hex_dump(20, 4));
            }
        }

        assert_eq!(magic_number, 0x7A7B7C7D);

        let metadata = self.metadata.as_ref().unwrap();
        if metadata.len() != ((number_of_blocks + 1) * block_size) as u64 {
            return olr_perr!("Invalid file size. ({} + 1) * {} != {} bytes. {}", number_of_blocks, block_size, metadata.len(), reader.to_error_hex_dump(24, 4));
        }
        
        Ok(())
    }

    fn get_redo_log_header(&self, read_buffer : &[u8]) -> Result<RedoLogHeader, OLRError> {
        // Validate block by its checksum
        self.validate_block(read_buffer)?;

        let mut reader = ByteReader::from_bytes(read_buffer);
        reader.set_endian(self.endian.unwrap());

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
        redo_log_header.resetlogs_id = reader.read_timestamp().unwrap();
        redo_log_header.resetlogs_scn = reader.read_scn().unwrap();
        redo_log_header.hws = reader.read_u32().unwrap();
        redo_log_header.thread = reader.read_u16().unwrap();
        reader.skip_bytes(2);
        redo_log_header.first_scn = reader.read_scn().unwrap();
        redo_log_header.first_time = reader.read_timestamp().unwrap();
        redo_log_header.next_scn = reader.read_scn().unwrap();
        redo_log_header.next_time = reader.read_timestamp().unwrap();
        redo_log_header.eot = reader.read_u8().unwrap();
        redo_log_header.dis = reader.read_u8().unwrap();
        redo_log_header.zero_blocks = reader.read_u8().unwrap();
        redo_log_header.format_id = reader.read_u8().unwrap();
        redo_log_header.enabled_scn = reader.read_scn().unwrap();
        redo_log_header.enabled_time = reader.read_timestamp().unwrap();
        redo_log_header.thread_closed_scn = reader.read_scn().unwrap();
        redo_log_header.thread_closed_time = reader.read_timestamp().unwrap();
        reader.skip_bytes(4);
        redo_log_header.misc_flags = reader.read_u32().unwrap();
        redo_log_header.terminal_recovery_scn = reader.read_scn().unwrap();
        redo_log_header.terminal_recovery_time = reader.read_timestamp().unwrap();
        reader.skip_bytes(8);
        redo_log_header.most_recent_scn = reader.read_scn().unwrap();
        redo_log_header.largest_lwn = reader.read_u32().unwrap();
        redo_log_header.real_next_scn = reader.read_scn().unwrap();
        redo_log_header.standby_apply_delay = reader.read_u32().unwrap();
        redo_log_header.prev_resetlogs_scn = reader.read_scn().unwrap();
        redo_log_header.prev_resetlogs_id = reader.read_timestamp().unwrap();
        redo_log_header.misc_flags_2 = reader.read_u32().unwrap();
        reader.skip_bytes(4);
        redo_log_header.standby_log_close_time = reader.read_timestamp().unwrap();
        reader.skip_bytes(124);
        redo_log_header.thr = reader.read_i32().unwrap();
        redo_log_header.seq2 = reader.read_i32().unwrap();
        redo_log_header.scn2 = reader.read_scn().unwrap();
        redo_log_header.redo_log_key.copy_from_slice( reader.read_bytes(16).unwrap().as_slice()); 
        reader.skip_bytes(16);
        redo_log_header.redo_log_key_flag = reader.read_u16().unwrap();
        reader.skip_bytes(30);

        Ok(redo_log_header)
    }

    fn validate_block(&self, read_buffer : &[u8]) -> Result<(), OLRError> {
        let checksum = self.block_checksum(read_buffer);
        if checksum != 0 {
            let mut reader = ByteReader::from_bytes(read_buffer);
            reader.skip_bytes(14);
            let block_checksum = reader.read_u16().unwrap();
            return olr_perr!("Bad block. Checksums are not equal: {} != {}. {}", checksum ^ block_checksum, block_checksum, reader.to_error_hex_dump(14, 2));
        }
        Ok(())
    }

    fn block_checksum(&self, read_buffer : &[u8]) -> u16 {
        let block_size: usize = self.block_size.unwrap();
        let mut reader = ByteReader::from_bytes(read_buffer);
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


