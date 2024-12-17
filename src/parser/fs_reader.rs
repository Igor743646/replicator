use std::{fs::{File, Metadata}, io::{Read, Seek}, path::PathBuf, sync::Arc, time};

use crossbeam::channel::Sender;
use log::{debug, info, warn};

use crate::{common::{errors::{OLRError, Result}, memory_pool::MemoryChunk, thread::Thread}, ctx::Ctx, olr_err, olr_perr};
use crate::common::OLRErrorCode::*;

use super::byte_reader::{ByteReader, Endian};

#[derive(Debug)]
pub enum ReaderMessage {
    Start(usize, Metadata, Endian),
    Read(MemoryChunk, usize),
    Eof,
}

pub(crate) struct Reader {
    context_ptr : Arc<Ctx>,
    file_path : PathBuf,
    sender : Sender<ReaderMessage>,
}

impl Reader {
    pub fn new(context_ptr : Arc<Ctx>, file_path : PathBuf, sender : Sender<ReaderMessage>) -> Self {
        Self {
            context_ptr,
            file_path,
            sender,
        }
    }

    fn get_chunk(&self) -> Result<MemoryChunk> {
        self.context_ptr.get_chunk()
    }

    fn free_chunk(&self, chunk : MemoryChunk) {
        self.context_ptr.free_chunk(chunk)
    }

    fn read_partial(&self, archive_log_file : &mut File, block_size : usize) -> std::result::Result<usize, (usize, OLRError)> {
        let mut read_size = 0;

        loop {
            let mut chunk = {
                let result = self.get_chunk();

                if let Err(err) = result {
                    return Err((read_size, err));
                }
                result.unwrap()
            };

            let result = archive_log_file.read(chunk.as_mut());

            if let Err(err) = result {
                self.free_chunk(chunk);
                return Err((read_size, olr_err!(FileReading, "Can not read archive log file. Err: {}", err)));
            }
            let mut size = result.unwrap();
            
            if size == 0 {
                self.free_chunk(chunk);
                if let Err(err) = self.sender.send(ReaderMessage::Eof) {
                    warn!("Problems while sending EOF info. Err: {}", err);
                    return Err((read_size, olr_err!(ChannelSend, "Can not send message: {}", err)));
                }
                debug!("Eof. Stop reading file.");
                break;
            }

            if size % block_size > 0 {
                let result = archive_log_file.seek_relative(-((size % block_size) as i64));

                if let Err(err) = result {
                    self.free_chunk(chunk);
                    return Err((read_size, olr_err!(FileReading, "Can not seek file: {}", err)));
                }

                size = (size / block_size) * block_size;
            }

            if size == 0 {
                warn!("Very small read data.");
                self.free_chunk(chunk);
                continue;
            }

            if let Err(err) = self.sender.send(ReaderMessage::Read(chunk, size)) {
                warn!("Problems while sending next partition. Err: {}", err);
                return Err((read_size, olr_err!(ChannelSend, "Can not send message: {}", err)));
            }
            read_size += size;
        }
        Ok(read_size)
    }

    fn get_file_data(&self, archive_log_file : &mut File) -> Result<(usize, Endian)> {
        let mut buf = [0u8; 512];

        let result = archive_log_file.read_exact(&mut buf);

        if let Err(err) = result {
            return olr_err!(FileReading, "Could not read file header. Err: {}", err);
        }

        let mut reader = ByteReader::from_bytes(&buf);
        reader.set_endian(Endian::LittleEndian);
        reader.skip_bytes(28);
        let magic_number = reader.read_u32().unwrap();
        match magic_number {
            0x7A7B7C7D => {},
            0x7D7C7B7A => { reader.set_endian(Endian::BigEndian); },
            _ => {
                return olr_perr!("Unknown magic number in file header. {}", reader.to_error_hex_dump(28, 4));
            },
        }
        reader.reset_cursor();
        reader.skip_bytes(20);
        let block_size = reader.read_u32().unwrap();
        
        if block_size == 0 {
            return olr_err!(FileReading, "Block size is zero");
        }

        Ok((block_size as usize, reader.endian()))
    }
}

impl Thread for Reader {
    fn alias(&self) -> String {
        format!("Reader thread. Path: {}", self.file_path.to_str().unwrap().to_string())
    }

    fn run(&self) -> Result<()> {

        let mut confirmed_size: usize = 0;
        let retry: i32 = 5;
        let mut last_retry: i32 = retry;
        let mut block_size: Option<usize> = None;

        loop {
            if last_retry <= 0 {
                return olr_err!(FileReading, "Couldn't read archive log file {} times. Shutdown...", retry);
            }

            info!("Openning archive log file: {:?}", self.file_path);
            let archive_log_file = File::open(&self.file_path);

            if let Err(err) = archive_log_file {
                last_retry -= 1;
                warn!("Could not open archive log file: {:?}. Err: {}. Sleep and retry...", self.file_path, err);
                std::thread::sleep(time::Duration::from_millis(100));
                continue;
            }
            let mut archive_log_file = archive_log_file.unwrap();

            if block_size.is_none() { // for the first loop cycle
                debug!("Get block size, metadata and endian");
                let metadata = archive_log_file.metadata();

                if let Err(err) = metadata {
                    last_retry -= 1;
                    warn!("Could not get metadata for checking file size. Err: {}. Retry...", err);
                    std::thread::sleep(time::Duration::from_millis(50));
                    continue;
                }
                let metadata = metadata.unwrap();

                if metadata.len() % 512 != 0 {
                    last_retry -= 1;
                    warn!("The file size is not a multiple of 512. File size: {}", metadata.len());
                    std::thread::sleep(time::Duration::from_millis(50));
                    continue;
                }
                
                let result = self.get_file_data(&mut archive_log_file);

                if let Err(err) = result {
                    last_retry -= 1;
                    warn!("Could not read block size from file header. Err: {}. Retry...", err);
                    std::thread::sleep(time::Duration::from_millis(50));
                    continue;
                }
                let result = result.unwrap();
                block_size = Some(result.0);

                if let Err(err) = self.sender.send(ReaderMessage::Start(result.0, metadata, result.1)) {
                    warn!("Problems while sending file info. Err: {}", err);
                    return olr_err!(ChannelSend, "Can not send message: {}", err);
                }
            }
            
            let seek_result = archive_log_file.seek(std::io::SeekFrom::Start(confirmed_size as u64));
            
            if let Err(err) = seek_result {
                warn!("Seek failed. Err: {}. Try reopen file...", err);
                continue;
            }
            last_retry = retry;

            let result = self.read_partial(&mut archive_log_file, block_size.unwrap());
            
            if let Err((size, err)) = result {
                warn!("Error while reading file. Err: {}. Read size: {}", err, size);
                confirmed_size += size;
                continue;
            }
            debug!("Confirm: {}", confirmed_size);

            confirmed_size += result.unwrap();
            assert!(confirmed_size == archive_log_file.metadata().unwrap().len() as usize);
            break;
        }

        while !self.sender.is_empty() {
            std::thread::sleep(time::Duration::from_millis(10));
        }

        Ok(())
    }
}
