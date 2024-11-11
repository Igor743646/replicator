use std::collections::VecDeque;
use std::path::PathBuf;
use log::{info, trace, warn};

use crate::{common::errors::OLRError, olr_err, parser::Parser};
use crate::common::OLRErrorCode::*;

trait ArchiveDigger {
    fn get_parsers_queue(&self) -> Result<VecDeque<Parser>, OLRError>;
}

struct ArchiveDiggerOffline {
    archive_log_format : String, 
    db_recovery_file_destination : String,
    context : String,
    last_checked_day : String,
    mapping_fn : Box<dyn Fn(PathBuf) -> PathBuf>,
}

impl ArchiveDiggerOffline {
    pub fn new(archive_log_format : String, db_recovery_file_destination : String,
        context : String, last_checked_day : String, mapping_fn : Box<dyn Fn(PathBuf) -> PathBuf>) -> Self {
        Self {
            archive_log_format, 
            db_recovery_file_destination,
            context,
            last_checked_day,
            mapping_fn,
        }
    }
}

impl ArchiveDigger for ArchiveDiggerOffline {
    fn get_parsers_queue(&self) -> Result<VecDeque<Parser>, OLRError> {
        if self.archive_log_format.is_empty() {
            return olr_err!(MissingFile, "Missing location of archived redo logs. Archive log format is empty.").into();
        }
        
        let mapped_path: PathBuf = [
            &self.db_recovery_file_destination, 
            &self.context, 
            "archivelog"
        ].iter().collect();
        let mapped_path = (self.mapping_fn)(mapped_path);

        if !mapped_path.is_dir() {
            return olr_err!(WrongDirName, "Not a directory: {}", mapped_path.display()).into();
        }

        trace!("Check path: {}", mapped_path.display());

        let directory = mapped_path.read_dir()
            .or(olr_err!(MissingDir, "Can not read directory: {}", mapped_path.display()).into())?;

        let parser_queue = VecDeque::new();

        for object in directory {
            let (object_path, object_type) = match object {
                Ok(entry) => {
                    (entry.path(), entry.file_type())
                },
                Err(err) => {
                    warn!("Error while iterate the dirrectory: {} error: {}", mapped_path.display(), err.to_string());
                    continue;
                }
            };

            info!("Check path: {} type: {:?}", object_path.display(), object_type);
        }

        Ok(parser_queue)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};
    use crate::{common::errors::OLRError, init_logger};
    use super::{ArchiveDigger, ArchiveDiggerOffline};

    fn create_offline_digger() -> ArchiveDiggerOffline {
        ArchiveDiggerOffline::new(
            "o1_mf_%t_%s_%h_.arc".to_string(),
            "".to_string(),
            "DB_NAME".to_string(),
            "".to_string(),
            Box::new(|path| -> PathBuf {
                match path.to_str().unwrap() {
                    r"/opt/oracle/fst/archivelog" => r"/data/d2/archivelog".into(),
                    r"DB_NAME\archivelog" => r"./archivelog".into(),
                    _ => path,
                }
            }),
        )
    }

    #[test]
    fn test_mapping_none() -> Result<(), OLRError> {
        let digger = create_offline_digger();
        assert_eq!((digger.mapping_fn)("".into()), PathBuf::from_str("").unwrap());
        Ok(())
    }

    #[test]
    fn test_mapping_ok() -> Result<(), OLRError> {
        let digger = create_offline_digger();
        assert_eq!((digger.mapping_fn)("/opt/oracle/fst/archivelog".into()), PathBuf::from_str("/data/d2/archivelog").unwrap());
        Ok(())
    }

    #[test]
    fn test_queue_getting() -> Result<(), OLRError> {
        init_logger();
        let digger = create_offline_digger();
        
        let _ = digger.get_parsers_queue()?;

        Ok(())
    }
}