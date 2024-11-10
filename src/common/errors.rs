use std::backtrace::Backtrace;

#[derive(Debug)]
pub struct OracleLogicalReplicatorError {
    code : i32,
    message : String,
    backtrace : String,
}

pub type OLRError = OracleLogicalReplicatorError;

impl OLRError {
    pub fn new(code : i32, message : String) -> Self {
        let backtrace = Backtrace::force_capture().to_string();
        Self { code, message, backtrace }
    }
}

impl<T> Into<Result<T, OLRError>> for OLRError {
    fn into(self) -> Result<T, OLRError> {
        Err(self)
    }
}

impl std::fmt::Display for OLRError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {:06} Description: {} Backtrace:\n{}", self.code, self.message, self.backtrace)
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
