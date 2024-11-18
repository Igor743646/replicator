use std::{fs::File, io::{Read, Seek}, path::PathBuf, sync::{Arc, RwLock}, time};

use crossbeam::channel::Sender;
use log::{debug, info, warn};

use crate::{common::{errors::OLRError, memory_pool::MemoryChunk, thread::Thread}, ctx::Ctx, olr_err, olr_perr};
use crate::common::OLRErrorCode::*;

use super::byte_reader::ByteReader;

pub enum ReaderMessage {
    Read(MemoryChunk, usize),
    Eof,
}

pub(crate) struct Reader {
    context_ptr : Arc<RwLock<Ctx>>,
    file_path : PathBuf,
    sender : Sender<ReaderMessage>,
}

impl Reader {
    pub fn new(context_ptr : Arc<RwLock<Ctx>>, file_path : PathBuf, sender : Sender<ReaderMessage>) -> Self {
        Self {
            context_ptr,
            file_path,
            sender,
        }
    }

    fn get_chunk(&self) -> Result<MemoryChunk, OLRError> {
        let mut context = self.context_ptr.write().unwrap();
        context.get_chunk()
    }

    fn free_chunk(&self, chunk : MemoryChunk) {
        let mut context = self.context_ptr.write().unwrap();
        context.free_chunk(chunk)
    }

    fn read_partial(&self, archive_log_file : &mut File, block_size : usize) -> Result<usize, (usize, OLRError)> {
        let mut read_size = 0;

        loop {
            let mut chunk = {
                let result = self.get_chunk();

                if let Err(err) = result {
                    return Err((read_size, err));
                }
                result.unwrap()
            };

            let r = archive_log_file.read(chunk.as_mut());

            if let Err(err) = r {
                self.free_chunk(chunk);
                return Err((read_size, olr_err!(FileReading, "Can not read file: {}", err.to_string())));
            }
            let mut size = r.unwrap();
            debug!("read {} bytes", size);

            if size == 0 {
                self.free_chunk(chunk);
                self.sender.send(ReaderMessage::Eof).unwrap();
                debug!("End read file");
                break;
            }

            if size % block_size > 0 {
                let result = archive_log_file.seek_relative(-((size % block_size) as i64));

                if let Err(err) = result {
                    self.free_chunk(chunk);
                    return Err((read_size, olr_err!(FileReading, "Can not seek file: {}", err.to_string())));
                }

                size = (size / block_size) * block_size;
            }

            self.sender.send(ReaderMessage::Read(chunk, size)).unwrap();
            read_size += size;
        }
        Ok(read_size)
    }

    fn get_block_size(&self, archive_log_file : &mut File) -> Result<usize, OLRError> {
        let mut buf = [0u8; 512];

        let result = archive_log_file.read_exact(&mut buf);

        if let Err(err) = result {
            return olr_err!(FileReading, "Can not read file header. Err: {}", err);
        }

        let mut reader = ByteReader::from_bytes(&mut buf);
        reader.set_endian(super::byte_reader::Endian::LittleEndian);
        reader.skip_bytes(28);
        let magic_number = reader.read_u32().unwrap();
        match magic_number {
            0x7A7B7C7D => {},
            0x7D7C7B7A => { reader.set_endian(super::byte_reader::Endian::BigEndian); },
            _ => {
                return olr_perr!("Unknown magic number in file header. {}", reader.to_error_hex_dump(28, 4));
            },
        }
        reader.reset_cursor();
        reader.skip_bytes(20);
        let block_size = reader.read_u32().unwrap();
        Ok(block_size as usize)
    }
}

impl Thread for Reader {
    fn alias(&self) -> String {
        format!("Reader thread. Path: {}", self.file_path.to_str().unwrap().to_string())
    }

    fn run(&self) -> Result<(), OLRError> {

        let mut confirmed_size = 0u64;
        let retry = 5;
        let mut last_retry = 5;
        let mut block_size: Option<usize> = None;

        loop {

            info!("Open file: {:?}", self.file_path);
            let archive_log_file = File::open(&self.file_path);

            if let Err(err) = archive_log_file {
                if last_retry == 0 {
                    return olr_err!(FileReading, "Can not open archive file {} times. Err: {}. Shutdown...", retry, err);
                }
                last_retry -= 1;
                warn!("Can not open archive file: {:?}. Sleep and retry...", self.file_path);
                std::thread::sleep(time::Duration::from_millis(100));
                continue;
            }
            let mut archive_log_file = archive_log_file.unwrap();

            if block_size.is_none() {
                let result = self.get_block_size(&mut archive_log_file);

                if let Err(err) = result {
                    last_retry -= 1;
                    warn!("Can not read block size from file header. Err: {}", err);
                    std::thread::sleep(time::Duration::from_millis(50));
                    continue;
                }
                block_size = result.ok();
            }
            
            let seek_result = archive_log_file.seek(std::io::SeekFrom::Start(confirmed_size));
            
            if let Err(err) = seek_result {
                warn!("Seek failed. Err: {}. Try reopen file...", err);
                continue;
            }
            last_retry = retry;

            let result = self.read_partial(&mut archive_log_file, block_size.unwrap());
            
            if let Err((size, err)) = result {
                warn!("Error while reading file. Err: {}. Read: {}", err, size);
                confirmed_size += size as u64;
                continue;
            }
            debug!("Confirm: {}", confirmed_size);

            confirmed_size += result.unwrap() as u64;
            assert!(confirmed_size == archive_log_file.metadata().unwrap().len());
            break;
        }

        Ok(())
    }
}
