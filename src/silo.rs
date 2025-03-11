//! Provides the silo, a file data store that stores de/serializable structs as
//! fixed length byte records in a collection of silo files.
use serde::{Serialize, Deserialize};
use std::fs;
use std::fs::{OpenOptions,File};
use std::vec::Vec;
use std::path::Path;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use thiserror::Error;


// ----------------------------------------------------------------

#[derive(Debug,Error)]
pub enum RecordStoreError {
    /// IO went wrong doing an operation
    #[error("An IO error occurred: {0}")]
    IoError(std::io::Error),
    /// Problem in a silo
    #[error("A silo error occurred: {0}")]
    Silo(String),
    /// Problem in a record store
    #[error("A record store error occurred: {0}")]
    RecordStore(String),
    /// Problem in an object store
    #[error("An object store error occurred: {0}")]
    ObjectStore(String),
}

impl From<std::io::Error> for RecordStoreError {
    fn from(error: std::io::Error) -> Self {
        RecordStoreError::IoError(error)
    }
}

// ----------------------------------------------------------------

/// encapsulates a number of files for storing records of type
/// Serialize + for<'de> Deserialize<'de>
pub struct Silo<T: Serialize + for<'de> Deserialize<'de>> {
    /// how many records this silo currently holds
    pub current_count: usize,

    record_size: usize,
    silo_dir: String,
    records_per_subsilo: usize,
    subsilos: Vec<File>,
    is_open: bool,
    _silo_type: PhantomData<T>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> Silo<T> {
    ///
    /// Create a new silo for the given type which must
    /// implement Serialize + for<'de> Deserialize<'de>
    ///
    /// # Arguments
    ///
    /// * `silo_dir` Directory silo files are located.
    /// * `record_size` Size of record in bytes.
    /// * `max_file_size` Maximum size of a silo file.
    ///
    /// # Returns
    ///
    /// * `Silo<T>` 
    ///
    pub fn new( silo_dir: String,
                record_size: usize,
                max_file_size: usize ) -> Silo<T>
    {
        Silo {
            current_count: 0,
            silo_dir,
            record_size,
            records_per_subsilo: max_file_size / record_size,
            subsilos: Vec::new(),
            is_open: false,
            _silo_type: PhantomData,
        }
    }

    ///
    /// Opens the silo if it is not already opened.
    /// Makes sure there is at least one silo file.
    /// Calculates how many records there are in this silo.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - successful open
    /// * `Error(ReocordStoreError::IoError)` when there is a filesystem problem
    ///
    pub fn open( &mut self ) -> Result<(), RecordStoreError>
    {
        if self.is_open {
            return Ok(())
        }

        //
        // open up the subsilo files, filtering by numeric filenames
        //
        let _ = ensure_path( &self.silo_dir );

        let mut dir_entries: Vec<std::fs::DirEntry> = std::fs::read_dir(&self.silo_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u32>().ok())
                    .is_some()
            })
            .collect();

        //
        // sort the dir entries numerically.
        //
        dir_entries.sort_by_key(|entry| {
            entry.file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
                .unwrap_or(u32::MAX)
        });

        //
        // add each entry into the subsilo entries
        // note: this ignores the case where there are 
        // gaps in the numbers
        //
        for entry in &dir_entries {
            self.subsilos.push( OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .open(entry.path())?
            );
        }
        


        //
        // load subsilo files and make sure there is at least one silo file
        //
        if self.subsilos.len() == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open( &[self.silo_dir.clone(),"0".to_string()].join("/") )?;
            self.subsilos.push( file );
        }

        //
        // count the number of records in the subsilos based on their size
        //
        let mut silo_size_big = 0;
        for file in &self.subsilos {
            silo_size_big += file.metadata()?.len();
        }

        let silo_size: usize = silo_size_big as usize;
        self.current_count = silo_size / self.record_size;
        
        self.is_open = true;

        Ok(())
    }

    ///
    /// Returns the subsilo file that corresponds to the given index.
    /// This ensures the file size will be large enough to handle the given index.
    ///
    /// # Arguments
    ///
    /// * `idx` the index of a record.
    ///
    fn subsilo_file_for_idx(&mut self, idx: usize) -> (&mut File,usize) {
        let subsilo_idx = idx / self.records_per_subsilo;
        let idx_in_subsilo =  idx % self.records_per_subsilo;
        let seek_position = idx_in_subsilo*self.record_size;
        let subsilo_file = self.subsilo(subsilo_idx).expect("could not get subsilo file");
        subsilo_file.seek(SeekFrom::Start(seek_position as u64)).expect("could not seek");
        (subsilo_file,idx_in_subsilo)
    }

    ///
    /// Pushes a record on to the end of the silo.
    ///
    /// # Arguments
    ///
    /// * `record` a struct that is de/serializeable.
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - new index in the silo.
    /// * `Error(ReocordStoreError::IoError)` when there is a filesystem problem
    ///
    pub fn push(&mut self, record: &T) -> Result<usize,RecordStoreError> {

        let new_id = self.current_count;

        let rs = self.record_size;

        let (subsilo_file,idx_in_subsilo) = self.subsilo_file_for_idx(new_id);
        let subsilo_space: u64 = ((1 + idx_in_subsilo) * rs).try_into().unwrap();

        let encoded: Vec<u8> = bincode::serialize(record).expect("Serialization failed");
        subsilo_file.write_all(&encoded).expect("push: not write record");

        if subsilo_file.metadata().expect("could not get file length").len() < subsilo_space {
            subsilo_file.set_len(subsilo_space).expect("push: could not extend file size");
        }

        self.current_count = 1 + self.current_count;

        Ok(new_id)
    }

    ///
    /// Writes a record to the given index which must already exist.
    ///
    /// # Arguments
    ///
    /// * `idx` 
    /// * `record` a struct that is de/serializeable.
    ///
    /// #Returns
    ///
    /// * `Ok(())` - Record was put
    /// * `Err(RecordStoreError::SiloError)` - index was out of bounds.
    ///
    pub fn put_record(&mut self, idx: usize, record: &T) -> Result<(),RecordStoreError> {
        if idx < self.current_count {
            let (subsilo_file,_idx_in_subsilo) = self.subsilo_file_for_idx(idx);
            let encoded: Vec<u8> = bincode::serialize(record).expect("Serialization failed");
            match subsilo_file.write_all(&encoded) {
                Err(err) => Err(RecordStoreError::IoError(err)),
                Ok(()) => Ok(()),
            }
        } else {
            let cc = self.current_count;
            Err(RecordStoreError::Silo(format!("put_record: idx {} must be lower than current count {}", idx, cc)))
        }
    }

    ///
    /// Returns a record from the given index.
    ///
    /// # Arguments
    ///
    /// * `idx` : index of record to look up
    ///
    /// #Returns
    ///
    /// * `None` - index was beyond the last record.
    /// * `T` - record struct.
    ///
    pub fn fetch_record(&mut self, idx: usize) -> Option<T> {
        if idx >= self.current_count {
            return None;
        }
        let (subsilo_file,_subsilo_idx) = self.subsilo_file_for_idx(idx);

        let mut buffer = Vec::new();
        subsilo_file.read_to_end(&mut buffer).expect("Reading failed");
        let record: T = bincode::deserialize(&buffer).expect("Deserialization failed");

        Some(record)
    }

    ///
    /// Removes the last record from this silo and returns it.
    ///
    /// #Returns
    ///
    /// * `None` - silo is empty
    /// * `T` - record struct.
    ///
    pub fn pop(&mut self) -> Option<T> {
        if self.current_count == 0 {
            return None
        }
        let rs = self.record_size;
        let last_idx = self.current_count - 1;

        let (subsilo_file,subsilo_idx) = self.subsilo_file_for_idx(last_idx);
        let subsilo_space: u64 = (subsilo_idx * rs).try_into().unwrap();

        let mut buffer = Vec::new();
        subsilo_file.read_to_end(&mut buffer).expect("Reading failed");
        let data: T = bincode::deserialize(&buffer).expect("Deserialization failed");

        subsilo_file.set_len(subsilo_space).expect("pop: could not set_len of subsilo");

        self.current_count = last_idx;
        Some(data)
    }


    ///
    /// Looks up the last record from this silo and returns it.
    ///
    /// #Returns
    ///
    /// * `None` - silo is empty
    /// * `T` - record struct.
    ///
    pub fn peek(&mut self) -> Option<T> {
        if self.current_count == 0 {
            return None
        }

        let last_idx = self.current_count - 1;
        let (subsilo_file,_subsilo_idx) = self.subsilo_file_for_idx(last_idx);

        let mut buffer = Vec::new();
        subsilo_file.read_to_end(&mut buffer).expect("Reading failed");
        let data: T = bincode::deserialize(&buffer).expect("Deserialization failed");

        Some(data)
    }

    ///
    /// Returns the silo file that would contain the given record by index
    /// , creating it if it does not exist.
    ///
    /// # Arguments
    ///
    /// * `idx` : index of record to look up silo file.
    ///
    /// #Returns
    ///
    /// * `OK(File)` - File struct
    /// * `Err(RecordStoreError::IoError)` - If a filesystem error occurs.
    ///
    fn subsilo(&mut self,idx: usize) -> Result<&mut File, RecordStoreError> {
        let mut len = self.subsilos.len() as usize;
        while len <= idx {
            let path = [self.silo_dir.clone(),len.to_string()].join("/");
            self.subsilos.push(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open( &path )? );
            len = len + 1;
        }
        Ok(self.subsilos.get_mut(idx as usize).unwrap())
    }

}

///
/// Make sure a directory path exists.
///
/// # Arguments
///
/// * `dir` : String representation of directory path to ensure.
///
/// #Returns
///
/// * `OK(())` - If the directory tree was created
/// * `Err(RecordStoreError::IoError)` - If a filesystem error occurs.
///
fn ensure_path(dir: &String) -> Result<(), RecordStoreError> {
    if ! Path::new(&dir).exists() {
        fs::create_dir_all(dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::recycle_silo::SiloByteData;

    #[test]
    fn plain_silo() {
        let record_size = 64*2;
        let max_file_size = 3 * record_size; // 3 records per file
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();

        let data: [u8; 6] = [0x01, 0x01, 0x02, 0x03, 0x05, 0x06];
        let more_bytes = SiloByteData::new(&data);

        let data: [u8; 5] = [0xCA, 0xFE, 0xBA, 0xBE, 0xEE];
        let my_bytes = SiloByteData::new( &data );

        let mut silo = Silo::<SiloByteData>::new( testdir_path.clone(), record_size, max_file_size );
        silo.open().expect("could not open silo");

        assert_eq!(silo.record_size, 128);
        assert_eq!(silo.current_count, 0);
        assert_eq!(silo.silo_dir, testdir_path);
        assert_eq!(silo.subsilos.len(), 1);

        match silo.fetch_record(2) {
            Some(_silo_bytes) => panic!("get record returns bytes when it should not"),
            None => assert_eq!(1, 1),
        }
        match silo.fetch_record(0) {
            Some(_silo_bytes) => panic!("get record returns bytes when it should not"),
            None => assert_eq!(1, 1),
        }

        let id = silo.push(&my_bytes).ok().unwrap();
        assert_eq!(id, 0);
        assert_eq!(silo.current_count, 1);
        assert_eq!(silo.subsilos.len(), 1);

        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), record_size.try_into().unwrap() );
            },
            Err(err) => panic!("get record returns nothing {err}")
        }
        match silo.fetch_record(0) {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ) ,
            None => panic!("get record returns nothing")
        }

        match silo.peek() {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ),
            None => panic!("peek returns nothing")
        }

        let id = silo.push(&more_bytes).ok().unwrap();
        assert_eq!(id, 1);
        assert_eq!(silo.current_count, 2);
        assert_eq!(silo.subsilos.len(), 1);
        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), (record_size * 2).try_into().unwrap() );
            },
            Err(err) => panic!("get record returns nothing {err}")
        }
        match silo.fetch_record(0) {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ),
            None => panic!("get record returns nothing")
        }
        match silo.fetch_record(1) {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("get record returns nothing")
        }
        match silo.fetch_record(2) {
            Some(_silo_bytes) => panic!("get record returns bytes when it should not"),
            None => assert_eq!(1, 1)
        }
        match silo.peek() {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("peek returns nothing")
        }

        let mut silo = Silo::<SiloByteData>::new( testdir_path.clone(), record_size, max_file_size );

        silo.open().expect("could not open silo");

        assert_eq!(silo.current_count, 2);
        assert_eq!(silo.subsilos.len(), 1);
        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), (record_size * 2).try_into().unwrap() );
            },
            Err(err) => panic!("get record returns nothing {err}")
        }
        match silo.fetch_record(0) {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ),
            None => panic!("get record returns nothing")
        }
        match silo.fetch_record(1) {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("get record returns nothing")
        }
        match silo.fetch_record(2) {
            Some(_silo_bytes) => panic!("get record returns bytes when it should not"),
            None => assert_eq!(1, 1)
        }
        match silo.peek() {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("peek returns nothing")
        }

        match silo.pop() {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("pop returns nothing")
        }
        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), (record_size * 1).try_into().unwrap() );
            },
            Err(err) => panic!("get record returns nothing {err}")
        }
        match silo.pop() {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ),
            None => panic!("pop doesnt returns nothing"),
        }
        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), 0 );
            },
            Err(err) => panic!("get record returns nothing {err}")
        }
        match silo.peek() {
            Some(_silo_bytes) => panic!("should be nothing to peek at"),
            None => assert_eq!( 0, 0 ),
        }
        match silo.pop() {
            Some(_silo_bytes) => panic!("pop doesnt returns nothing"),
            None => assert_eq!( 0, 0 ),
        }
        let id = silo.push(&more_bytes).ok().unwrap();
        assert_eq!(id, 0);
        assert_eq!(silo.current_count, 1);
        assert_eq!(silo.subsilos.len(), 1);

        let id = silo.push(&my_bytes).unwrap();
        assert_eq!(id, 1);
        assert_eq!(silo.current_count, 2);
        assert_eq!(silo.subsilos.len(), 1);

        let id = silo.push(&my_bytes).unwrap();
        assert_eq!(id, 2);
        assert_eq!(silo.current_count, 3);
        assert_eq!(silo.subsilos.len(), 1);

        let id = silo.push(&more_bytes).unwrap();
        assert_eq!(id, 3);
        assert_eq!(silo.current_count, 4);
        assert_eq!(silo.subsilos.len(), 2);

        match silo.fetch_record(2) {
            Some(silo_bytes) => assert_eq!( silo_bytes, my_bytes ),
            None => panic!("get record returns nothing")
        }

        match silo.fetch_record(3) {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("get record returns nothing")
        }

	match silo.put_record( 2, &more_bytes ) {
            Ok(()) => assert!(true),
            Err(_err) => panic!("unable to put record"),
        }

	match silo.put_record( 4, &more_bytes ) {
            Ok(()) => panic!("able to put record when it should not be able to"),
            Err(_err) => assert!(true),
        }

        match silo.fetch_record(2) {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("get record returns nothing")
        }

        let mut silo = Silo::<SiloByteData>::new( testdir_path.clone(), record_size, max_file_size );
        silo.open().expect("could not reopen silo");
        
        match silo.fetch_record(2) {
            Some(silo_bytes) => assert_eq!( silo_bytes, more_bytes ),
            None => panic!("get record returns nothing")
        }
        

    }
}
