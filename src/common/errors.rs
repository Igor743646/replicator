use std::backtrace::Backtrace;

#[derive(Debug, Copy, Clone)]
pub enum OLRErrorCode {
    Internal = 1,
    WrongFileName = 100000,
    GetFileMetadata,
    FileReading,
    FileWriting,
    FileDeserialization,
    FileSerialization,
    UnknownConfigField,
    MissingConfigField,
    WrongConfigFieldType,
    NotValidField,
    ChannelSend = 200000,
    ChannelRecv,
    UnknownCharset,
    TakeLock,
    MemoryAllocation,
    ThreadSpawn,
}

#[derive(Debug)]
pub struct OracleLogicalReplicatorError {
    code : OLRErrorCode,
    message : String,
    backtrace : String,
}

pub type OLRError = OracleLogicalReplicatorError;

impl OLRError {
    pub fn new(code : OLRErrorCode, message : String) -> Self {
        let backtrace: Backtrace = Backtrace::force_capture();
        Self { code, message, backtrace : backtrace.to_string() }
    }
}

impl<T> Into<Result<T, OLRError>> for OLRError {
    fn into(self) -> Result<T, OLRError> {
        Err(self)
    }
}

impl std::fmt::Display for OLRError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {:06} Description: {} Backtrace:\n{}", self.code as i32, self.message, self.backtrace)
    }
}

#[macro_export]
macro_rules! olr_err {
    ($code:tt, $message:expr, $($args:tt)*) => {
        $crate::common::errors::OLRError::new($code, format!($message, $($args)*))
    };

    ($code:tt, $message:expr) => {
        $crate::common::errors::OLRError::new($code, format!($message))
    };
}
