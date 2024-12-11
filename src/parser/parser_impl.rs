use core::fmt;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::fmt::Display;
use std::fs::{File, Metadata};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use std::path::PathBuf;

use clap::error::Result;
use log::{info, trace, warn};

use crate::builder::JsonBuilder;
use crate::common::constants;
use crate::common::thread::spawn;
use crate::common::types::{TypeRBA, TypeRecordScn, TypeScn, TypeTimestamp};
use crate::ctx::Ctx;
use crate::olr_perr;
use crate::parser::fs_reader::{Reader, ReaderMessage};
use crate::parser::opcodes::opcode0501::OpCode0501;
use crate::parser::opcodes::opcode0502::OpCode0502;
use crate::parser::opcodes::opcode0504::OpCode0504;
use crate::parser::opcodes::opcode0520::OpCode0520;
use crate::parser::opcodes::opcode1102::OpCode1102;
use crate::parser::opcodes::{VectorInfo, VectorParser};
use crate::parser::record_analizer::RecordAnalizer;
use crate::parser::record_reader::VectorReader;
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl Display for RedoRecordHeaderExpansion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Num/NumMax: {}/{} Records count: {}\nRecord SCN: {}\nSCN1: {}\nSCN2: {}\nTimestamp: {}", 
                self.record_num, self.record_num_max, self.records_count, self.records_scn, self.scn1, self.scn2, self.records_timestamp)
    }
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

impl Display for RedoRecordHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record size: {} VLD: {:02X}\nRecord SCN: {} Sub SCN: {}\n", self.record_size, self.vld, self.scn, self.sub_scn)?;
        if let Some(con_id) = self.container_uid {
            write!(f, "Container id: {}\n", con_id)?;
        }
        if let Some(ref ext) = self.expansion {
            write!(f, "{}\n", ext)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct RedoVectorHeaderExpansion {
    pub container_id : u16,
    pub flag : u16,
}

#[derive(Debug, Default)]
pub struct RedoVectorHeader {
    pub op_code : (u8, u8),
    pub class : u16,
    pub afn : u16, // absolute file number
    pub dba : u32,
    pub vector_scn : TypeScn,
    pub seq : u8,  // sequence number
    pub typ : u8,  // change type
    pub expansion : Option<RedoVectorHeaderExpansion>,

    pub fields_count : u16,
    pub fields_sizes : Vec<u16>,
}

impl Display for RedoVectorHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let opcode_desc = match self.op_code {
            (5, 1) => "Operation info (undo block redo)",
            (5, 2) => "Begin transaction / iternal (undo header redo)",
            (5, 4) => "Commit / rollback",
            (5, 6) => "Rollback record index in an undo block",
            (5, 11) => "Rollback DBA in transaction table entry",
            (5, 19) | (5, 20) => "Session info",
            (10, 2) => "Insert leaf row",
            (10, 8) => "Initialize new leaf block",
            (10, 18) => "Update keydata in row",
            (11, 2) => "Insert row piece",
            (11, 3) => "Delete row piece",
            (11, 4) => "Lock row piece",
            (11, 5) => "Update row piece",
            (11, 6) => "Overwrite row piece",
            (11, 8) => "Change forwarding address",
            (11, 11) => "Insert multiply rows",
            (11, 12) => "Delete multiply rows",
            (11, 16) => "Logminer support - RM for rowpiece with only logminer columns",
            (11, 17) => "Update multiply rows",
            (11, 22) => "Logminer support",
            (19, 1) => "Direct block logging",
            (24, 1) => "Common portion of the ddl",
            (26, 2) => "Generic lob redo",
            (26, 6) => "Direct lob direct-load redo",

            (4, _) => "Transaction block",
            (5, _) => "Transaction undo",
            (10, _) => "Transaction index",
            (13, _) => "Transaction segment",
            (14, _) => "Transaction extent",
            (17, _) => "Recovery (REDO)",
            (18, _) => "Hot Backup Log Blocks",
            (22, _) => "Tablespace bitmapped file operations",
            (23, _) => "Write behind logging of blocks",
            (24, _) => "Logminer related (DDL or OBJV# redo)",
            (_, _) => "Unknown",
        };
        write!(f, "OpCode: {}.{} ({})\n", self.op_code.0, self.op_code.1, opcode_desc)?;
        write!(f, "Class: {} Absolute file number: {} DBA: {}\n", self.class, self.afn, self.dba)?;
        write!(f, "Vector SCN: {} SEQ: {} TYP: {}\n", self.vector_scn, self.seq, self.typ)?;
        if let Some(ref ext) = self.expansion {
            write!(f, "Container id: {} Flag: {}\n", ext.container_id, ext.flag)?;
        }
        write!(f, "Fields count: {}\n", self.fields_count)?;
        write!(f, "Fields sizes: {}\n", self.fields_sizes.iter().map(|x| -> String {format!("{} ", x)}).collect::<String>())
    }
}

#[derive(Debug)]
pub struct Parser {
    context_ptr : Arc<Ctx>,
    builder_ptr : Arc<JsonBuilder>,
    file_path : PathBuf,
    sequence : TypeSeq,

    block_size      : Option<usize>,
    version         : Option<u32>,
    endian          : Option<byte_reader::Endian>,
    metadata        : Option<Metadata>,
    dump_log_level  : u64,
    dump_file       : Option<File>,

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
    pub fn new(context_ptr : Arc<Ctx>, builder_ptr : Arc<JsonBuilder>, file_path : PathBuf, sequence : TypeSeq) -> Self {
        let mut dump_file: Option<File> = None;
        if context_ptr.dump.level > 0 {
            let directory: PathBuf = PathBuf::new().join(context_ptr.dump.path.as_str());
            let dump_path: PathBuf = directory.join(format!("dump-{}.ansi", sequence));
            std::fs::create_dir_all(directory).unwrap();
            dump_file = Some(File::create(dump_path).unwrap());
        }
        Self {
            context_ptr: context_ptr.clone(), 
            builder_ptr,
            file_path, 
            sequence,
            block_size : None,
            version : None,
            endian : None, 
            metadata : None,
            dump_log_level : context_ptr.dump.level,
            dump_file,
            records_manager : RecordsManager::new(context_ptr.clone()),
        }
    }

    pub fn can_dump(&self, level : u64) -> bool {
        level <= self.dump_log_level
    }

    pub fn write_dump(&mut self, fmt: fmt::Arguments<'_>) {
        self.dump_file.as_ref().unwrap().write_fmt(fmt).unwrap();
    }

    pub fn sequence(&self) -> TypeSeq {
        self.sequence
    }

    pub fn version(&self) -> Option<u32> {
        self.version
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
        let mut timestamp : TypeTimestamp = Default::default();
        let mut record : Option<&mut Record> = None;

        loop {
            let message: ReaderMessage = rx.recv().unwrap();

            let (chunk, blocks_count) = match message {
                ReaderMessage::Read(chunk, size) => {
                    assert!(size >= 512);
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
                    if self.can_dump(1) {
                        self.write_dump(format_args!("{:#?}", redo_log_header));
                    }
                    self.version = Some(redo_log_header.oracle_version);
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
                    end_block = start_block + redo_record_header.expansion.as_ref().unwrap().records_count as usize;
                    timestamp = redo_record_header.expansion.as_ref().unwrap().records_timestamp.clone();

                    reader.set_cursor(16)?;
                }

                while reader.cursor() < self.block_size.unwrap() {
                    if to_read == 0 {
                        if reader.cursor() + 20 >= self.block_size.unwrap() {
                            break;
                        }

                        let prev_offset: usize = reader.cursor();
                        let redo_record_header: RedoRecordHeader = match reader.read_redo_record_header(redo_log_header.oracle_version) {
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
                        record.as_mut().unwrap().timestamp = timestamp.clone();

                        reader.set_cursor(prev_offset)?;
                    }

                    let to_copy = std::cmp::min(to_read, self.block_size.unwrap() - reader.cursor());

                    let buffer = &mut record
                            .as_mut()
                            .unwrap()
                            .data_mut()[record_position .. record_position + to_copy];

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

                        self.analize_record(record.unwrap(), redo_log_header.oracle_version)?;
                    }

                    self.records_manager.free_chunks();
                }
                start_block += 1;
            }

            {
                info!("Processed chunk");
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

        redo_log_header.block_header = reader.read_block_header()?;
        reader.skip_bytes(4);
        redo_log_header.oracle_version = reader.read_u32()?;
        redo_log_header.database_id = reader.read_u32()?;
        redo_log_header.database_name = String::from_utf8(reader.read_bytes(8)?).unwrap();
        redo_log_header.control_sequence = reader.read_u32()?;
        redo_log_header.file_size = reader.read_u32()?;
        reader.skip_bytes(4);
        redo_log_header.file_number = reader.read_u16()?;
        reader.skip_bytes(2);
        redo_log_header.activation_id = reader.read_u32()?;
        reader.skip_bytes(36);
        redo_log_header.description = String::from_utf8(reader.read_bytes(64)?).unwrap();
        redo_log_header.blocks_count = reader.read_u32()?;
        redo_log_header.resetlogs_id = reader.read_timestamp()?;
        redo_log_header.resetlogs_scn = reader.read_scn()?;
        redo_log_header.hws = reader.read_u32()?;
        redo_log_header.thread = reader.read_u16()?;
        reader.skip_bytes(2);
        redo_log_header.first_scn = reader.read_scn()?;
        redo_log_header.first_time = reader.read_timestamp()?;
        redo_log_header.next_scn = reader.read_scn()?;
        redo_log_header.next_time = reader.read_timestamp()?;
        redo_log_header.eot = reader.read_u8()?;
        redo_log_header.dis = reader.read_u8()?;
        redo_log_header.zero_blocks = reader.read_u8()?;
        redo_log_header.format_id = reader.read_u8()?;
        redo_log_header.enabled_scn = reader.read_scn()?;
        redo_log_header.enabled_time = reader.read_timestamp()?;
        redo_log_header.thread_closed_scn = reader.read_scn()?;
        redo_log_header.thread_closed_time = reader.read_timestamp()?;
        reader.skip_bytes(4);
        redo_log_header.misc_flags = reader.read_u32()?;
        redo_log_header.terminal_recovery_scn = reader.read_scn()?;
        redo_log_header.terminal_recovery_time = reader.read_timestamp()?;
        reader.skip_bytes(8);
        redo_log_header.most_recent_scn = reader.read_scn()?;
        redo_log_header.largest_lwn = reader.read_u32()?;
        redo_log_header.real_next_scn = reader.read_scn()?;
        redo_log_header.standby_apply_delay = reader.read_u32()?;
        redo_log_header.prev_resetlogs_scn = reader.read_scn()?;
        redo_log_header.prev_resetlogs_id = reader.read_timestamp()?;
        redo_log_header.misc_flags_2 = reader.read_u32()?;
        reader.skip_bytes(4);
        redo_log_header.standby_log_close_time = reader.read_timestamp()?;
        reader.skip_bytes(124);
        redo_log_header.thr = reader.read_i32()?;
        redo_log_header.seq2 = reader.read_i32()?;
        redo_log_header.scn2 = reader.read_scn()?;
        redo_log_header.redo_log_key.copy_from_slice( reader.read_bytes(16)?.as_slice()); 
        reader.skip_bytes(16);
        redo_log_header.redo_log_key_flag = reader.read_u16()?;
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


impl RecordAnalizer for Parser {
    fn analize_record(&mut self, record_ptr : *mut Record, version : u32) -> Result<(), OLRError> {
        
        let record = unsafe { record_ptr.as_mut().unwrap() };

        trace!("Analize block: {} offset: {} scn: {} subscn: {}", record.block, record.offset, record.scn, record.sub_scn);

        let mut reader = ByteReader::from_bytes(record.data());
        reader.set_endian(self.endian.unwrap());
        let record_header = reader.read_redo_record_header(version)?;
        
        if record_header.record_size != record.size {
            return olr_perr!("Sizes must be equal, but: {} != {}", record_header.record_size, record.size);
        }

        if self.can_dump(1) {
            self.write_dump(format_args!("\n\n########################################################\n"));
            self.write_dump(format_args!("#                     REDO RECORD                      #\n"));
            self.write_dump(format_args!("########################################################\n"));
            self.write_dump(format_args!("\nHeader: {}", record_header));
        }

        let mut vector_info_pull : VecDeque<VectorInfo> = VecDeque::with_capacity(2);
        while !reader.eof() {
            let vector_header: RedoVectorHeader = reader.read_redo_vector_header(version)?;
            trace!("Analize vector: {:?} offset: {}", vector_header.op_code, reader.cursor());
            reader.align_up(4);

            if self.can_dump(1) {
                self.write_dump(format_args!("\n{}", vector_header));
            }

            let vector_body_size : usize = vector_header.fields_sizes
                .iter()
                .map(|x| {
                    ((*x + 3) & !3) as usize
                })
                .sum();

            let vec_reader = VectorReader::new(
                vector_header, 
                &reader.data()[reader.cursor() .. reader.cursor() + vector_body_size]
            );

            reader.skip_bytes(vector_body_size);

            let vector_info = match vec_reader.header.op_code {
                (5, 1) => OpCode0501::parse(self, vec_reader)?,
                (5, 2) => OpCode0502::parse(self, vec_reader)?,
                (5, 4) => OpCode0504::parse(self, vec_reader)?,
                (5, 20) => OpCode0520::parse(self, vec_reader)?,
                (11, 2) => OpCode1102::parse(self, vec_reader)?,
                (a, b) => {
                    warn!("Opcode: {}.{} not implemented", a, b); 
                    continue;
                },
            };
            vector_info_pull.push_back(vector_info);
            
            if vector_info_pull.len() == 2 {

                let first = vector_info_pull.pop_front().unwrap();
                let second = vector_info_pull.pop_front().unwrap();

                match (first, second) {
                    (VectorInfo::OpCode0501(opcode0501), VectorInfo::OpCode1102(opcode1102)) => {
                        self.builder_ptr.process_insert(record.scn, record.timestamp, opcode0501, opcode1102)?;
                    },
                    (a, b) => {
                        info!("Unknown pair: {} {}", a, b);
                        warn!("Can not process it");
                    }
                }

                vector_info_pull.clear();
            }

            if vector_info_pull.len() == 1 {

                match vector_info_pull.front().unwrap() {
                    VectorInfo::OpCode0502(begin) => {
                        if begin.xid.sequence_number != 0 { // else INTERNAL
                            self.builder_ptr.process_begin(record.scn, record.timestamp, begin.xid)?;
                        }
                        
                        vector_info_pull.clear();
                    },
                    VectorInfo::OpCode0504(commit) => {
                        let is_rollback = commit.flg & constants::FLAG_KTUCF_ROLLBACK != 0;
                        self.builder_ptr.process_commit(record.scn, record.timestamp, commit.xid, is_rollback)?;
                        vector_info_pull.clear();
                    },
                    _ => (),
                }

            }
        }

        vector_info_pull.clear();
        Ok(())
    }
}