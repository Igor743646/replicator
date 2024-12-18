use core::fmt;
use std::collections::VecDeque;
use std::fs::{File, Metadata, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;
use std::path::PathBuf;

use crossbeam::channel::Receiver;
use log::{info, trace, warn};

use crate::builder::JsonBuilder;
use crate::common::thread::spawn;
use crate::common::types::TypeTimestamp;
use crate::ctx::Ctx;
use crate::olr_perr;
use crate::parser::archive_structs::record_header::RecordHeader;
use crate::parser::fs_reader::{Reader, ReaderMessage};
use crate::parser::opcodes::{Vector, VectorKind};
use crate::parser::record_analizer::RecordAnalizer;
use crate::parser::records_manager::Record;
use crate::transactions::transaction_buffer::TransactionBuffer;
use crate::{common::types::TypeSeq, olr_err};
use crate::common::errors::OLRErrorCode::*;
use crate::common::errors::Result;

use super::archive_structs::redolog_header::RedoLogHeader;
use super::byte_reader::{self, ByteReader};
use super::records_manager::RecordsManager;

#[derive(Debug)]
pub struct Parser {
    context_ptr : Arc<Ctx>,
    builder_ptr : Arc<JsonBuilder>,
    transaction_buffer : Arc<Mutex<TransactionBuffer>>,
    file_path : PathBuf,
    sequence : TypeSeq,

    block_size      : Option<usize>,
    version         : Option<u32>,
    endian          : Option<byte_reader::Endian>,
    metadata        : Option<Metadata>,
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

struct ParserState<'a> {
    pub to_read : usize,
    pub start_block : usize,
    pub end_block : usize,
    pub redo_log_header : RedoLogHeader,
    pub record_position : usize,
    pub timestamp : TypeTimestamp,
    pub record : Option<&'a mut Record>
}

impl Parser {
    pub fn new(context_ptr : Arc<Ctx>, builder_ptr : Arc<JsonBuilder>, transaction_buffer : Arc<Mutex<TransactionBuffer>>, file_path : PathBuf, sequence : TypeSeq) -> Result<Self> {
        let mut result = Self {
            context_ptr: context_ptr.clone(), 
            builder_ptr,
            transaction_buffer,
            file_path, 
            sequence,
            block_size : None,
            version : None,
            endian : None, 
            metadata : None,
            dump_file : None,
            records_manager : RecordsManager::new(context_ptr.clone()),
        };

        if context_ptr.dump.level > 0 {
            let directory_path: PathBuf = PathBuf::new().join(context_ptr.dump.path.as_str());
            let dump_file_path: PathBuf = directory_path.join(format!("dump-{}.ansi", sequence));
            if let Err(err) = std::fs::create_dir_all(&directory_path) {
                return olr_err!(CreateDir, "Can not create directory: {:?}. Error: {}", directory_path, err);
            }
            match OpenOptions::new().create(true).write(true).open(dump_file_path) {
                Err(err) => return olr_err!(CreateDir, "Can not create directory: {:?}. Error: {}", directory_path, err),
                Ok(file) => result.dump_file = Some(file),
            }
        }
        
        Ok(result)
    }

    pub fn can_dump(&self, level : u64) -> bool {
        level <= self.context_ptr.dump.level
    }

    pub fn write_dump(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        if let Some(ref mut file) = self.dump_file {
            if let Err(err) = file.write_fmt(fmt) {
                return olr_err!(FileWriting, "Can not write dump in file. Error: {}", err);
            }
        }
        Ok(())
    }

    pub fn dump_column(&mut self, bytes : &[u8], col_num : usize, col_len : usize, is_null : bool) -> Result<()> {
        if is_null {
            self.write_dump(format_args!("Col {:>3} [{}]: NULL\n", col_num, col_len))?;
        } else {
            self.write_dump(format_args!("Col {:>3} [{}]: {:02X?}\n", col_num, col_len, &bytes[.. col_len as usize] ))?;
        }
        Ok(())
    }

    pub fn sequence(&self) -> TypeSeq {
        self.sequence
    }

    pub fn version(&self) -> Option<u32> {
        self.version
    }

    fn start_reader(&self) -> Result<(Receiver<ReaderMessage>, JoinHandle<Result<()>>)> {
        let (sx, rx) = self.context_ptr.get_reader_channel();
        let fs_reader = Reader::new(self.context_ptr.clone(), self.file_path.clone(), sx);
        let handle = spawn(fs_reader)?;
        let result = (rx, handle);
        Ok(result)
    }

    pub fn parse(&mut self) -> Result<()> {

        let start_parsing_time = Instant::now();
        let (rx, fs_reader_handle) = self.start_reader()?;

        let message = rx.recv().unwrap();

        match message {
            ReaderMessage::Start(block_size, metadata, endian) => {
                self.block_size = block_size.into();
                self.endian = endian.into();
                self.metadata = metadata.into();
            },
            data => return olr_err!(ChannelRecv, "Wrong data in first message: {:?}", data),
        }

        let mut state = ParserState {
            to_read : 0,
            start_block : 0,
            end_block : 0,
            redo_log_header : Default::default(),
            record_position : 0,
            timestamp : Default::default(),
            record :  None,
        };

        loop {
            let message: ReaderMessage = rx.recv().unwrap();

            let (chunk, blocks_count) = match message {
                ReaderMessage::Read(chunk, size) => {
                    assert!(size >= 512);
                    (chunk, size / self.block_size.unwrap())
                },
                ReaderMessage::Eof => break,
                _ => return olr_err!(ChannelRecv, "Unexpected message type: {:?}", message),
            };

            for idx in 0 .. blocks_count {
                let range = idx * self.block_size.unwrap() .. (idx + 1) * self.block_size.unwrap();
                self.process_block(&mut state, &chunk[range])?;
            }

            info!("Processed chunk");
            self.context_ptr.free_chunk(chunk);
        }

        assert!(self.records_manager.records_count() == 0);

        info!("Time elapsed: {:?}", start_parsing_time.elapsed());
        fs_reader_handle.join().unwrap()?;
        Ok(())
    }

    fn process_block(&mut self, state : &mut ParserState, phisical_block : &[u8]) -> Result<()> {
        if state.start_block == 0 {
            self.check_file_header(&phisical_block)?;
            state.start_block += 1;
            state.end_block += 1;
            return Ok(());
        }

        if state.start_block == 1 {
            state.redo_log_header = self.get_redo_log_header(&phisical_block)?;
            if self.can_dump(1) {
                self.write_dump(format_args!("{:#?}", state.redo_log_header))?;
            }
            self.version = Some(state.redo_log_header.oracle_version);
            state.start_block += 1;
            state.end_block += 1;
            return Ok(());
        }

        let mut reader = ByteReader::from_bytes(&phisical_block);
        reader.set_endian(self.endian.unwrap());

        reader.skip_bytes(16); // Skip block header
        
        if state.start_block == state.end_block {
            let record_header = match reader.read_record_header(state.redo_log_header.oracle_version) {
                Ok(x) => x,
                Err(err) => return olr_perr!("Parse record header error: {}. {}", err, reader.to_error_hex_dump(16, 68))
            };

            assert!(record_header.expansion.is_some(), "Dump: {}", reader.to_error_hex_dump(16, 68));
            let record_expansion = record_header.expansion.as_ref().unwrap();
            state.end_block = state.start_block + record_expansion.records_count as usize;
            state.timestamp = record_expansion.records_timestamp.clone();

            reader.set_cursor(16)?;
        }

        while reader.cursor() < self.block_size.unwrap() {
            if state.to_read == 0 {
                if reader.cursor() + 20 >= self.block_size.unwrap() {
                    break;
                }

                let prev_offset: usize = reader.cursor();
                let redo_record_header: RecordHeader = match reader.read_record_header(state.redo_log_header.oracle_version) {
                    Ok(x) => x,
                    Err(err) => return olr_perr!("Parse record header error: {}. {}", err, reader.to_error_hex_dump(16, 24))
                };

                if redo_record_header.record_size == 0 {
                    break;
                }

                state.to_read = redo_record_header.record_size as usize;
                state.record_position = 0;

                let reserved_record = self.records_manager.reserve_record(state.to_read)?;
                reserved_record.scn = redo_record_header.scn;
                reserved_record.sub_scn = redo_record_header.sub_scn;
                reserved_record.block = state.start_block as u32;
                reserved_record.offset = prev_offset as u16;
                reserved_record.size = redo_record_header.record_size;
                reserved_record.timestamp = state.timestamp.clone();
                state.record = Some(reserved_record);

                reader.set_cursor(prev_offset)?;
            }

            let to_copy = std::cmp::min(state.to_read, self.block_size.unwrap() - reader.cursor());

            let buffer = &mut state.record
                    .as_mut()
                    .unwrap()
                    .data_mut()[state.record_position .. state.record_position + to_copy];

            assert!(buffer.len() == to_copy);
            reader.read_bytes_into(to_copy, buffer).unwrap();
            
            state.to_read -= to_copy;
            state.record_position += to_copy;
        }

        if state.start_block + 1 == state.end_block {
            // Process data here

            while let Some(record) = self.records_manager.drop_record() {
                self.analize_record(record)?;
            }

            self.records_manager.free_chunks();
        }
        state.start_block += 1;

        Ok(())
    }

    fn check_file_header(&self, buffer : &[u8]) -> Result<()> {
        assert!(self.block_size.is_some());
        assert!(self.endian.is_some());

        let mut reader = ByteReader::from_bytes(&buffer);
        reader.set_endian(self.endian.unwrap());

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

    fn get_redo_log_header(&self, read_buffer : &[u8]) -> Result<RedoLogHeader> {
        // Validate block by its checksum
        self.validate_block(read_buffer)?;

        let mut reader = ByteReader::from_bytes(read_buffer);
        reader.set_endian(self.endian.unwrap());

        assert!(reader.data().len() >= 512, "Block length {} < 512", reader.data().len());

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

    fn validate_block(&self, read_buffer : &[u8]) -> Result<()> {
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

    fn push_to_transaction_begin(&mut self, record : &Record, begin : Vector) -> Result<()> {
        let xid = begin.xid().unwrap();

        if xid.sequence_number == 0 { // INTERNAL
            return Ok(());
        }

        let mut guard = self.transaction_buffer.lock().unwrap();
        guard.init_transaction(xid, record.scn, record.timestamp)?;
        Ok(())
    }

    fn push_to_transaction_double(&mut self, vector1 : Vector, vector2 : Vector) -> Result<()> {
        let mut guard: std::sync::MutexGuard<'_, TransactionBuffer> = self.transaction_buffer.lock().unwrap();
        let xid = vector1.xid().expect("vector1 must be an opcode with xid");
        
        guard.add_double_in_transaction(xid, vector1, vector2)?;
        Ok(())
    }

    fn push_to_transaction_commit(&mut self, commit : Vector) -> Result<()> {
        let mut guard = self.transaction_buffer.lock().unwrap();
        // guard.close_transaction(commit.xid)?;

        // std::unimplemented!("Commit");

        Ok(())
    }
}


impl RecordAnalizer for Parser {
    fn analize_record(&mut self, record : &Record) -> Result<()> {
        
        trace!("Analize record: block: {} offset: {} scn: {} subscn: {}", record.block, record.offset, record.scn, record.sub_scn);

        let mut reader = ByteReader::from_bytes(record.data());
        reader.set_endian(self.endian.unwrap());
        let record_header = reader.read_record_header(self.version.unwrap())?;
        
        if record_header.record_size != record.size {
            return olr_perr!("Sizes must be equal, but: {} != {}", record_header.record_size, record.size);
        }

        if self.can_dump(1) {
            self.write_dump(format_args!("\n\n########################################################\n"))?;
            self.write_dump(format_args!("#                     REDO RECORD                      #\n"))?;
            self.write_dump(format_args!("########################################################\n"))?;
            self.write_dump(format_args!("\nHeader: {}\n", record_header))?;
        }

        let mut vector_pull : VecDeque<Vector> = VecDeque::with_capacity(2);
        while !reader.eof() {
            let vector = Vector::parse(self, &mut reader, self.version.unwrap())?;
            
            vector_pull.push_back(vector);
            
            if vector_pull.len() == 2 {

                let first = vector_pull.pop_front().unwrap();
                let second = vector_pull.pop_front().unwrap();

                match (first.kind(), second.kind()) {
                    (VectorKind::OpCode0501, VectorKind::OpCode1102) => {
                    },
                    (VectorKind::OpCode0501, VectorKind::OpCode0520) => {
                        self.push_to_transaction_double(first, second)?;
                    },
                    (_, _) => {
                        info!("Unknown pair: {:?} {:?}", first.kind(), second.kind());
                        warn!("Can not process it");
                    }
                }

                vector_pull.clear();
            }

            if vector_pull.len() == 1 {

                let first = vector_pull.front().unwrap();

                match first.kind() {
                    VectorKind::OpCode0502 => {
                        self.push_to_transaction_begin(record, vector_pull.pop_front().unwrap())?;
                    },
                    VectorKind::OpCode0504 => {
                        self.push_to_transaction_commit(vector_pull.pop_front().unwrap())?;
                    },
                    _ => (),
                }

            }
            
        }

        vector_pull.clear();
        Ok(())
    }
}