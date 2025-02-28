
//use named_lock;
//use std::io;

use std::path::Path;
//use std::path::PathBuf;
use std::fs;
use std::fs::File;

#[derive(Debug)]
pub enum RecordStoreError {
    None,
    IoError(std::io::Error),
    LockError(named_lock::Error),
}

/*
 * Connect errors to RecordStoreErrors

impl From<named_lock::Error> for RecordStoreError {
    fn from(error: named_lock::Error) -> Self {
        RecordStoreError::LockError(error)
    }
}

impl From<std::io::Error> for RecordStoreError {
    fn from(error: std::io::Error) -> Self {
        RecordStoreError::IoError(error)
    }
}

pub fn ensure_path(dir: &String) -> Result<(), RecordStoreError> {
    if ! Path::new(&dir).exists() {
        fs::create_dir(dir)?;
    }
    Ok(())
}

pub fn touch(file_path: &String) -> Result<File, RecordStoreError> {
    let path = Path::new(file_path);
    if path.exists() {
        Ok(fs::File::open(path)?)
    } else {
        Ok(fs::File::create(path)?)
    }
}


 */
