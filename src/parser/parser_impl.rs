use core::time;
use std::collections::VecDeque;
use std::fmt::Display;
use std::fs::Metadata;
use std::io::{self, BufReader, Seek};
use std::num::{NonZero, NonZeroU32};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::{fs::File, io::Read, path::PathBuf};

use crossbeam::channel;
use clap::error::Result;
use log::{debug, info, warn};

use crate::common::constants;
use crate::common::thread::spawn;
use crate::common::types::{TypeRBA, TypeRecordScn, TypeScn, TypeTimestamp};
use crate::ctx::Ctx;
use crate::olr_perr;
use crate::parser::fs_reader::{Reader, ReaderMessage};
use crate::{common::{errors::OLRError, types::TypeSeq}, olr_err};
use crate::common::errors::OLRErrorCode::*;

use super::byte_reader::{self, ByteReader, Endian::*};

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
    context_ptr : Arc<RwLock<Ctx>>,
    file_path : PathBuf,
    sequence : TypeSeq,

    block_size : Option<usize>,
    endian     : Option<byte_reader::Endian>,
    metadata   : Option<Metadata>,
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
    pub fn new(context : Arc<RwLock<Ctx>> , file_path : PathBuf, sequence : TypeSeq) -> Self {
        Self {context_ptr: context, file_path, sequence, block_size : None, endian : None, metadata : None }
    }

    pub fn sequence(&self) -> TypeSeq {
        self.sequence
    }

    pub fn parse(&mut self) -> Result<(), OLRError> {

        let start_parsing_time = Instant::now();

        let (sx, rx) = crossbeam::channel::bounded::<ReaderMessage>(constants::READER_CHANNEL_CAPACITY);
        let fs_reader = Reader::new(self.context_ptr.clone(), self.file_path.clone(), sx);
        let fs_reader_handle = spawn(fs_reader)?;

        let message = rx.recv().unwrap();

        match message {
            ReaderMessage::Start(block_size, metadata, endian) => {
                self.block_size = Some(block_size);
                self.endian = endian.into();
                self.metadata = metadata.into();
            },
            data => return olr_err!(ChannelSend, "Wrong data in first message: {:?}", data),
        }


        let mut records : VecDeque<Vec<u8>> = VecDeque::with_capacity(100);
        let mut to_read: usize = 0;
        let mut start_block: usize = 0;
        let mut end_block: usize = 0;
        let mut redo_log_header: RedoLogHeader = Default::default();

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
                let phisical_block = &chunk[idx * self.block_size.unwrap() .. (idx + 1) * self.block_size.unwrap()];
                if start_block == 0 {
                    self.check_file_header(&phisical_block)?;
                    start_block += 1;
                    end_block += 1;
                    continue;
                }

                if start_block == 1 {
                    redo_log_header = self.get_redo_log_header(&phisical_block)?;
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
                        let mut record = Vec::with_capacity(to_read);
                        unsafe { record.set_len(to_read); }
                        record.resize(to_read, Default::default());
                        records.push_back(record);
                        reader.reset_cursor();
                        reader.skip_bytes(prev_offset);
                    }

                    let to_copy = to_read.min(self.block_size.unwrap() - reader.cursor());
                    let record = records.back_mut();
                    
                    if record.is_none() {
                        return olr_perr!("Records are empty, but must be not empty. {}", reader.to_error_hex_dump(reader.cursor(), to_copy));
                    };
                    let record = record.unwrap();
                    let already_read = record.len() - to_read;
                    let mut record = &mut record.as_mut_slice()[already_read..];

                    if let Err(err) = reader.read_bytes_into(to_copy, &mut record) {
                        return olr_perr!("Can not write enough bytes to record: {} Reserved: {} Copy: {}. {}", record.len(), to_copy, err, reader.to_error_hex_dump(reader.cursor(), to_copy));
                    }
                    
                    to_read -= to_copy;
                }

                if start_block + 1 == end_block {
                    records.clear();
                }
                start_block += 1;
            }

            {
                let mut context = self.context_ptr.write().unwrap();
                context.free_chunk(chunk);
            }
        }

        for record in records.iter() {
            let mut reader = ByteReader::from_bytes(record);
            reader.set_endian(self.endian.unwrap());
            let size = reader.read_u32().unwrap();
            assert!(size == record.len() as u32, "{} {}", size, record.len() as u32);
        }

        info!("Time elapsed: {:?}", start_parsing_time.elapsed());
        fs_reader_handle.join().unwrap()?;
        Ok(())
    }

    fn check_file_header(&mut self, buffer : &[u8]) -> Result<(), OLRError> {
        let mut reader = ByteReader::from_bytes(&buffer);

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
            reader.reset_cursor();
        }

        let block_flag = reader.read_u8().unwrap();
        let file_type = reader.read_u8().unwrap();
        reader.skip_bytes(18);
        let block_size = reader.read_u32().unwrap();
        let number_of_blocks = reader.read_u32().unwrap();
        let _magic_number = reader.read_u32().unwrap();

        if block_flag != 0 {
            return olr_perr!("Invalid block flag: {}, expected 0x00. {}", block_flag, reader.to_error_hex_dump(0, 1));
        }

        match (file_type, block_size) {
            (0x22, 512) | (0x22, 1024) | (0x82, 4096) => {
                self.block_size = Some(block_size as usize);
            },
            _ => {
                return olr_perr!("Invalid block size: {}, expected one of {{512, 1024, 4096}}. {}", block_size, reader.to_error_hex_dump(20, 4));
            }
        }

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


