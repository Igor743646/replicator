use std::backtrace::Backtrace;

#[derive(Debug, Copy, Clone)]
pub enum OLRErrorCode {
    Internal = 1,
    WrongFileName = 100000,
    WrongDirName,
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
    ChannelSend = 200000,
    ChannelRecv,
    UnknownCharset,
    TakeLock,
    MemoryAllocation,
    ThreadSpawn,
}

#[derive(Debug)]
pub struct OracleLogicalReplicatorError {
    source      : &'static str,
    line        : u32,
    code        : OLRErrorCode,
    message     : String,
    backtrace   : String,
}

pub type OLRError = OracleLogicalReplicatorError;

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

impl<T> Into<Result<T, OLRError>> for OLRError {
    fn into(self) -> Result<T, OLRError> {
        Err(self)
    }
}

impl std::fmt::Display for OLRError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Src: {} Line: {}] Code: {:06} Description: {} Backtrace: {}", self.source, self.line, self.code as i32, self.message, self.backtrace)
    }
}

#[macro_export]
macro_rules! olr_err {
    ($code:tt, $message:expr, $($args:tt)*) => {
        $crate::common::errors::OLRError::new(file!(), line!(), $code, format!($message, $($args)*))
    };

    ($code:tt, $message:expr) => {
        $crate::common::errors::OLRError::new(file!(), line!(), $code, format!($message))
    };
}
