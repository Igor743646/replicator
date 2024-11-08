use std::thread::JoinHandle;

use crate::olr_err;

use super::errors::OLRError;

pub trait Thread 
    where Self : Send 
{
    fn run(&self) -> Result<(), OLRError>;
    fn alias(&self) -> &String;
}

pub fn spawn(thread : Box<dyn Thread + Send>) -> Result<JoinHandle<Result<(), OLRError>>, OLRError> {
    let alias = thread.alias().to_string();

    let handle = std::thread::Builder::new()
        .name(alias.clone())
        .spawn(move || -> Result<(), OLRError> {
            thread.run()?;
            Ok(())
        })
        .or_else(|err| olr_err!(040001, "Error while spawn thread {}: {}", alias, err.to_string()).into())?;

    Ok(handle)
}
