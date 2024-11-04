use std::backtrace::Backtrace;

#[derive(Debug)]
pub struct OracleDBReplicatorError {
    code : u32,
    message : String,
    backtrace : String,
}

impl OracleDBReplicatorError {
    pub fn new(code : u32, message : String) -> Self {
        let backtrace = Backtrace::force_capture().to_string();
        Self { code, message, backtrace }
    }

    pub fn err<T>(self) -> Result<T, Self> {
        Err(self)
    }
}

impl std::fmt::Display for OracleDBReplicatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {:06} Description: {} Backtrace:\n{}", self.code, self.message, self.backtrace)
    }
}
