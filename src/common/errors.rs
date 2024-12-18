use std::{backtrace::Backtrace, error::Error};
use std::fmt::{Formatter, Debug, Display};

#[derive(Debug, Copy, Clone)]
pub enum OLRErrorCode {
    Internal = 1,
    WrongFileName = 100000,
    WrongDirName,
    CreateDir,
    GetFileMetadata,
    FileReading,
    FileWriting,
    FileDeserialization,
    FileSerialization,
    UnknownConfigField,
    MissingConfigField,
    WrongConfigFieldType,
    NotValidField,
    MissingFile,
    MissingDir,
    ParseError,
    ChannelSend = 200000,
    ChannelRecv,
    UnknownCharset,
    TakeLock,
    MemoryAllocation,
    ThreadSpawn,
    OracleConnection,
    OracleQuery,
    SchemaReading,
    TransactionStart = 300000,
    TransactionClose,
    TransactionMemory,
}

#[derive(Debug)]
pub struct OLRError {
    source      : &'static str,
    line        : u32,
    code        : OLRErrorCode,
    message     : String,
    backtrace   : String,
}

pub type Result<T> = std::result::Result<T, OLRError>;

impl OLRError {
    pub fn new(source : &'static str, line : u32, code : OLRErrorCode, message : String) -> Self {
        let backtrace: Backtrace = Backtrace::capture();
        let backtrace = match backtrace.status() {
            std::backtrace::BacktraceStatus::Unsupported => "No backtrace".to_string(),
            std::backtrace::BacktraceStatus::Disabled => "No backtrace".to_string(),
            std::backtrace::BacktraceStatus::Captured => "\n".to_string() + backtrace.to_string().as_str(),
            _ => std::unreachable!(),
        };
        Self { source, line, code, message, backtrace }
    }
}

impl<T> Into<Result<T>> for OLRError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

impl Display for OLRError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Src: {}:{}] Code: \x1b[91m{:06}\x1b[0m Description: \x1b[95m{}\x1b[0m Backtrace: {}", self.source, self.line, self.code as i32, self.message, self.backtrace)
    }
}

impl Error for OLRError {}

#[macro_export]
macro_rules! olr_err {
    ($code:tt, $message:expr, $($args:tt)*) => {
        $crate::common::errors::OLRError::new(file!(), line!(), $code, format!($message, $($args)*)).into()
    };

    ($code:tt, $message:expr) => {
        $crate::common::errors::OLRError::new(file!(), line!(), $code, format!($message)).into()
    };
}

#[macro_export]
macro_rules! olr_perr {
    ($message:expr, $($args:tt)*) => {
        $crate::common::errors::OLRError::new(file!(), line!(), crate::common::errors::OLRErrorCode::ParseError, format!($message, $($args)*)).into()
    };

    ($message:expr) => {
        $crate::common::errors::OLRError::new(file!(), line!(), crate::common::errors::OLRErrorCode::ParseError, format!($message)).into()
    };
}
