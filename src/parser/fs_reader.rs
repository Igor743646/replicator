use std::{fs::File, io::Read, path::PathBuf, sync::{Arc, RwLock}, time};

use crossbeam::channel::Sender;
use log::{debug, info};

use crate::{common::{errors::OLRError, memory_pool::MemoryChunk, thread::Thread}, ctx::Ctx, olr_err};
use crate::common::OLRErrorCode::*;

pub enum ReaderMessage {
    Read(Arc<RwLock<MemoryChunk>>, usize),
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
}

impl Thread for Reader {
    fn alias(&self) -> String {
        format!("Reader thread. Path: {}", self.file_path.to_str().unwrap().to_string())
    }

    fn run(&self) -> Result<(), OLRError> {

        let mut archive_log_file = File::open(&self.file_path)
                .or(olr_err!(FileReading, "Can not open archive file"))?;

        let chunk = Arc::new(RwLock::new({
            let mut context = self.context_ptr.write().unwrap();
            context.get_chunk()?
        }));

        loop {
            let mut write_chunk = chunk.write().unwrap();
            let r = archive_log_file.read(write_chunk.as_mut());

            if let Err(err) = r {
                return olr_err!(FileReading, "Can not read file: {}", err.to_string());
            }
            let size = r.unwrap();

            if size == 0 {
                debug!("End read file");
                break;
            }

            self.sender.send(ReaderMessage::Read(chunk.clone(), size)).unwrap();

        }

        self.sender.send(ReaderMessage::Eof).unwrap();

        while Arc::<RwLock<MemoryChunk>>::strong_count(&chunk) > 1 {
            std::thread::sleep(time::Duration::from_millis(10));
        }
        info!("Chunk has been parsed");

        let a = Arc::into_inner(chunk).unwrap();
        let b = RwLock::into_inner(a).unwrap();

        {
            let mut context = self.context_ptr.write().unwrap();
            context.free_chunk(b);
        }

        Ok(())
    }
}
