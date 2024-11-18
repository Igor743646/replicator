use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use log::{info, trace, warn};

use crate::common::types::TypeSeq;
use crate::ctx::Ctx;
use crate::{common::errors::OLRError, olr_err, parser::parser_impl::Parser};
use crate::common::OLRErrorCode::*;

pub trait ArchiveDigger where Self: Send + Sync + Debug {
    fn get_parsers_queue(&self) -> Result<BinaryHeap<Reverse<Parser>>, OLRError>;
    fn get_sequence_from_file(&self, log_archive_format : &String, file : &PathBuf) -> Option<u32>;
}

pub struct ArchiveDiggerOffline {
    context_ptr : Arc<RwLock<Ctx>>,
    archive_log_format : String, 
    db_recovery_file_destination : String,
    db_name : String,
    min_sequence : Option<TypeSeq>,
    mapping_fn : Box<dyn Fn(PathBuf) -> PathBuf>,
}

impl Debug for ArchiveDiggerOffline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArchiveDiggerOffline {{ archive_log_format : {}, db_recovery_file_destination : {}, db_name : {}, min_sequence : {:?}}}", self.archive_log_format, self.db_recovery_file_destination, self.db_name, self.min_sequence)
    }
}

unsafe impl Send for ArchiveDiggerOffline {}
unsafe impl Sync for ArchiveDiggerOffline {}

impl ArchiveDiggerOffline {
    pub fn new(context : Arc<RwLock<Ctx>>, archive_log_format : String, db_recovery_file_destination : String,
        db_name : String, min_sequence : Option<TypeSeq>, mapping_fn : Box<dyn Fn(PathBuf) -> PathBuf>) -> Self {
        Self {
            context_ptr: context,
            archive_log_format, 
            db_recovery_file_destination,
            db_name,
            min_sequence,
            mapping_fn,
        }
    }
}

impl ArchiveDigger for ArchiveDiggerOffline {
    fn get_parsers_queue(&self) -> Result<BinaryHeap<Reverse<Parser>>, OLRError> {
        if self.archive_log_format.is_empty() {
            return olr_err!(MissingFile, "Missing location of archived redo logs. Archive log format is empty.");
        }
        
        let mapped_path: PathBuf = [
            &self.db_recovery_file_destination, 
            &self.db_name, 
            "archivelog"
        ].iter().collect();
        let mapped_path = (self.mapping_fn)(mapped_path);

        if !mapped_path.is_dir() {
            return olr_err!(WrongDirName, "Not a directory: {}", mapped_path.display());
        }

        trace!("Check path: {}", mapped_path.display());

        let directory = mapped_path.read_dir()
            .or(olr_err!(MissingDir, "Can not read directory: {}", mapped_path.display()))?;

        let mut parser_queue = BinaryHeap::new();

        for object in directory {
            if let Err(err) = object {
                warn!("Error while iterate the directory: {} error: {}", mapped_path.display(), err.to_string());
                continue;
            }

            let (object_path, object_type) = match object {
                Ok(entry) => {
                    (entry.path(), entry.file_type())
                },
                _ => std::unreachable!()
            };

            if let Err(err) = object_type {
                warn!("Error while getting metadata of file: {} error: {}", object_path.display(), err.to_string());
                continue;
            }

            let file_type = unsafe { object_type.unwrap_unchecked() };

            if !file_type.is_dir() {
                continue;
            }

            info!("Check path: {}", object_path.display());

            let directory = object_path.read_dir()
                .or(olr_err!(MissingDir, "Can not read directory: {}", object_path.display()))?;

            for archive_file in directory {
                if let Err(err) = archive_file {
                    warn!("Error while iterate the directory: {} error: {}", object_path.display(), err.to_string());
                    continue;
                }
                let archive_file = unsafe { archive_file.ok().unwrap_unchecked().path() };

                let sequence = self.get_sequence_from_file(&self.archive_log_format, &archive_file);
                
                if sequence.is_none() {
                    warn!("Bad sequence parsing of file: {}", archive_file.display());
                    continue;
                }

                let sequence: TypeSeq = unsafe { sequence.unwrap_unchecked().into() };

                if self.min_sequence.is_some() && sequence < self.min_sequence.unwrap() {
                    info!("Skip sequence {}", sequence);
                    continue;
                }

                info!("Found sequence: {:?}", sequence);

                parser_queue.push(Reverse(Parser::new(self.context_ptr.clone(), archive_file, sequence)));
            }
        }

        Ok(parser_queue)
    }

    fn get_sequence_from_file(&self, log_archive_format : &String, file : &PathBuf) -> Option<u32> {
        let log_archive_format = log_archive_format.as_bytes();
        let binding = file.file_name().unwrap().to_str().unwrap().to_string();
        let file = binding.as_bytes();
        
        let mut sequence : u32 = 0;
        let mut i = 0;
        let mut j = 0;

        while i < log_archive_format.len() && j < file.len() {
            if log_archive_format[i] == b'%' {
                if i + 1 >= log_archive_format.len() {
                    return None;
                }
                let mut digits = 0;
                if b"strdSTa".contains(&log_archive_format[i + 1]) {
                    // Some [0-9]*
                    let mut number: u32 = 0;
                    while j < file.len() && file[j] >= b'0' && file[j] <= b'9' {
                        number = number * 10 + (file[j] - b'0') as u32;
                        j += 1;
                        digits += 1;
                    }

                    if log_archive_format[i + 1] == b's' || log_archive_format[i + 1] == b'S' {
                        sequence = number as u32;
                    }
                    i += 2;
                } else if log_archive_format[i + 1] == b'h' {
                    // Some [0-9a-z]*
                    while j < file.len() && ((file[j] >= b'0' && file[j] <= b'9') || (file[j] >= b'a' && file[j] <= b'z')) {
                        j += 1;
                        digits += 1;
                    }
                    i += 2;
                }

                if digits == 0 {
                    return None;
                }
            } else if file[j] == log_archive_format[i] {
                i += 1;
                j += 1;
            } else {
                return None;
            }
        }

        if i == log_archive_format.len() && j == file.len() {
            return Some(sequence);
        }

        return None;
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr, sync::{Arc, RwLock}};
    use crate::{common::errors::OLRError, ctx::{Ctx, Dump}, init_logger};
    use super::{ArchiveDigger, ArchiveDiggerOffline};

    fn create_offline_digger(min_seq : u64) -> ArchiveDiggerOffline {
        let context: Arc<RwLock<crate::ctx::Ctx>> = Arc::new(RwLock::new(Ctx::new(
            Dump::default(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 1).unwrap()));
        ArchiveDiggerOffline::new(
            context,
            "o1_mf_%t_%s_%h_.arc".to_string(),
            "".to_string(),
            "DB_NAME".to_string(),
            Some(min_seq as u32),
            Box::new(|path| -> PathBuf {
                match path.to_str().unwrap() {
                    r"/opt/oracle/fst/archivelog" => r"/data/d2/archivelog".into(),
                    r"DB_NAME\archivelog" => r"./archivelog".into(),
                    r"DB_NAME/archivelog" => r"./archivelog".into(),
                    _ => path,
                }
            }),
        )
    }

    #[test]
    fn test_mapping_none() -> Result<(), OLRError> {
        let digger = create_offline_digger(0);
        assert_eq!((digger.mapping_fn)("".into()), PathBuf::from_str("").unwrap());
        Ok(())
    }

    #[test]
    fn test_mapping_ok() -> Result<(), OLRError> {
        let digger = create_offline_digger(0);
        assert_eq!((digger.mapping_fn)("/opt/oracle/fst/archivelog".into()), PathBuf::from_str("/data/d2/archivelog").unwrap());
        Ok(())
    }

    #[test]
    fn test_queue_getting() -> Result<(), OLRError> {
        init_logger();
        let q1 = create_offline_digger(0).get_parsers_queue()?;
        assert_eq!(q1.len(), 6);


        let q2 = create_offline_digger(173).get_parsers_queue()?;
        assert_eq!(q2.len(), 3);

        Ok(())
    }
}