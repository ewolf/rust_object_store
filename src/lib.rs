//pub mod record_store;

use serde::{Serialize, Deserialize};
use std::fs;
use std::fs::{OpenOptions,File};
use std::mem;
use std::vec::Vec;
use std::path::Path;
use std::io::{Read, Seek, SeekFrom, /*Error,*/ Write};
use std::marker::PhantomData;
use thiserror::Error;

#[derive(Debug,Error)]
pub enum RecordStoreError {
    #[error("An IO error occurred: {0}")]
    IoError(std::io::Error),
    #[error("A silo error occurred: {0}")]
    Silo(String),
    #[error("A record store error occurred: {0}")]
    RecordStore(String),
//    #[error("An unknown error occurred")]
//    Unknown,
}

impl From<std::io::Error> for RecordStoreError {
    fn from(error: std::io::Error) -> Self {
        RecordStoreError::IoError(error)
    }
}


// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

const RECORD_QUANTA   : usize = 4_096; // records must be multiples of this
const QUANTA_BOOST    : usize = 4;      // how many record quanta to jump between silos
const MAX_FILE_SIZE   : usize = 2_000_000_000;
const MIN_SILO_ID     : usize = 2;
const MAX_RECORD_SIZE : usize = MAX_FILE_SIZE / 15;
const MAX_SILO_ID     : usize = MAX_RECORD_SIZE / (RECORD_QUANTA * QUANTA_BOOST);


/*
 * takes a silo id and returns how big that silo is.
 */
fn size_for_silo_id(silo_id: usize) -> usize {
    RECORD_QUANTA * QUANTA_BOOST * silo_id
}

/*
 * takes a data size, a header size and a min silo id and gives the silo id that would assigned.
 */

fn silo_id_for_size(data_write_size: usize) -> usize {

    let mut silo_id = data_write_size / (RECORD_QUANTA*QUANTA_BOOST);

    if size_for_silo_id(silo_id) < data_write_size {
        silo_id = silo_id + 1;
    }
    if silo_id < MIN_SILO_ID {
        silo_id = MIN_SILO_ID;
    }
    silo_id
}

pub struct RecordStore {
    base_dir: String,
    data_silo_dir: String,
    data_silos: Vec<Option<RecycleSilo>>,
    index_silo: Silo<RecordIndexData>,
}


impl RecordStore {
    pub fn open(base_dir: &str) -> Result<RecordStore, RecordStoreError> {
	let data_silo_dir = [base_dir,"data_silos"].join("/");
        let index_silo_dir = [base_dir,"data_index"].join("/");
        let index_silo = Silo::open( index_silo_dir,
                                     mem::size_of::<RecordIndexData>().try_into().unwrap(),
                                     MAX_FILE_SIZE )?;

        let mut data_silos: Vec<Option<RecycleSilo>> = Vec::new();
        data_silos.resize_with(MAX_SILO_ID as usize + 1, || None);

        Ok (RecordStore {
            base_dir: base_dir.to_string(),
            data_silo_dir,
            data_silos,
            index_silo,
        })
    }
/*
    pub fn push(&mut self, data: &[u8] ) -> Result<usize,RecordStoreError> {
        let record = SiloByteData::new( &data );
        let silo_id = silo_id_for_size( record.size() );

        let data_silo = self.get_data_silo( silo_id )?;


        self.index_silo.push
    }
*/

/*
    fn new_data_silo(&mut self, silo_id: usize) -> Result<&RecycleSilo, RecordStoreError> {
        let silo = RecycleSilo::open( self.data_silo_dir,
                                      size_for_silo_id(silo_id),
                                      MAX_FILE_SIZE )?;
        self.data_silos[silo_id as usize] = silo;
        Ok(&silo)
    }
    fn get_data_silo(&mut self, id: usize) -> Result<&RecycleSilo, RecordStoreError> {
        if id > MAX_SILO_ID {
            return Err(RecordStoreError::RecordStore("get_data_silo: requested id {id} when max is {MAX_SILO_ID}".to_string()));
        }
        match self.data_silos.get(id as usize) {
            Some(opt) => {
                match opt {
                    Some(silo) => Ok(silo),
                    None => Ok(self.new_data_silo(id)?)
                }
            }
            None => Ok(self.new_data_silo(id)?)
        }
    }
*/

/*
    pub fn fetch(&mut self, idx: usize) -> Option<Vec<u8>> {

    }
    pub fn stow(&mut self, idx: usize) -> Option<Vec<u8>> {

    }
*/
}

// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SiloIdData {
    id: usize,
}
impl SiloIdData {
    pub fn new( id: usize ) -> SiloIdData {
         SiloIdData {
            id
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct RecordIndexData {
    silo_idx: usize,
    idx_in_silo: usize,
    // add timestamp here?
}
impl RecordIndexData {
    pub fn new( silo_idx: usize, idx_in_silo: usize ) -> RecordIndexData {
        RecordIndexData {
            silo_idx,
            idx_in_silo,
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SiloByteData {
    data_length: usize,
    bytes: Vec<u8>,
}

impl SiloByteData {
    pub fn new( data: &[u8] ) -> SiloByteData {
        SiloByteData {
            data_length: data.len(),
            bytes: data.to_vec(),
        }
    }
    pub fn size(&self) -> usize {
        (std::mem::size_of::<usize>() + self.bytes.len()).try_into().unwrap()
    }
}


// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

pub struct Silo<T: Serialize + for<'de> Deserialize<'de>> {
    current_count: usize,
    record_size: usize,
    silo_dir: String,
    records_per_subsilo: usize,
    subsilos: Vec<File>,
    _silo_type: PhantomData<T>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> Silo<T> {

    pub fn open(
        silo_dir: String,
        record_size: usize,
        max_file_size: usize ) -> Result<Silo<T>, RecordStoreError>
    {
        //
        // open up the subsilo files and tally their sizes
        //
        let _ = ensure_path( &silo_dir );

        //
        // load subsilo files and make sure there is at least one silo file
        //
        let mut subsilos = silo_files_in_directory(&silo_dir)?;

        if subsilos.len() == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open( &[silo_dir.clone(),"0".to_string()].join("/") )?;
            subsilos.insert( 0, file );
        }

        //
        // count the number of records in the subsilos based on their size
        //
        let mut silo_size_big = 0;
        for file in &subsilos {
            silo_size_big += file.metadata()?.len();
        }

        let silo_size: usize = silo_size_big as usize;
        let current_count = silo_size / record_size;
        Ok( Silo {
            current_count,
            silo_dir,
            record_size,
            records_per_subsilo: max_file_size / record_size,
            subsilos,
            _silo_type: PhantomData,
        } )
    }

    //
    // Returns the subsilo file that corresponds to the given index.
    // This ensures the file size will be large enough to handle the given index.
    //
    fn subsilo_file_for_idx(&mut self, idx: usize) -> (&mut File,usize) {
        let subsilo_idx = idx / self.records_per_subsilo;
        let idx_in_subsilo =  idx % self.records_per_subsilo;
        let seek_position = idx_in_subsilo*self.record_size;
        let subsilo_file = self.subsilo(subsilo_idx).expect("could not get subsilo file");
        eprintln!("SEEK TO  {seek_position} for index {idx} in subsilo {subsilo_idx} position {idx_in_subsilo}");
        subsilo_file.seek(SeekFrom::Start(seek_position as u64)).expect("could not seek");
        eprintln!("DONE SEEEK");
        (subsilo_file,idx_in_subsilo)
    }


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

        let cc = self.current_count;
        eprintln!("push done, current count now {cc}");


        Ok(new_id)
    }

    pub fn put_record(&mut self, id: usize, record: &T) -> Result<(),RecordStoreError> {
        if id < self.current_count {
            let (subsilo_file,_idx_in_subsilo) = self.subsilo_file_for_idx(id);
            let encoded: Vec<u8> = bincode::serialize(record).expect("Serialization failed");
            match subsilo_file.write_all(&encoded) {
                Err(err) => Err(RecordStoreError::IoError(err)),
                Ok(()) => Ok(()),
            }
        } else {
            let cc = self.current_count;
            Err(RecordStoreError::Silo(format!("put_record: idx {} must be lower than current count {}", id, cc)))
        }
    }

    pub fn fetch_record(&mut self, idx: usize) -> Option<T> {
        if idx >= self.current_count {
            return None;
        }
        let (subsilo_file,_subsilo_idx) = self.subsilo_file_for_idx(idx);

        let mut buffer = Vec::new();
        subsilo_file.read_to_end(&mut buffer).expect("Reading failed");
        let data: T = bincode::deserialize(&buffer).expect("Deserialization failed");

        Some(data)
    }

    pub fn pop(&mut self) -> Option<T> {
        let cc = self.current_count;
        eprintln!("pop start, current count  {cc}");
        if self.current_count == 0 {
            eprintln!("pop return none");
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
        eprintln!("pop return buffer, current count now {last_idx}");
        Some(data)
    }

    pub fn peek(&mut self) -> Option<T> {
        let cc = self.current_count;
        eprintln!("peek start, current count  {cc}");
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

    fn subsilo(&mut self,idx: usize) -> Result<&mut File, RecordStoreError> {
        let mut len = self.subsilos.len() as usize;
        while len <= idx {
            self.subsilos.push(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open( &[self.silo_dir.clone(),len.to_string()].join("/") )?);
            len = len + 1;
        }
        Ok(self.subsilos.get_mut(idx as usize).unwrap())
    }

}

// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

pub struct RecycleSilo {
    data_silo: Silo<SiloByteData>,
    recycler_silo: Silo<SiloIdData>,
}

impl RecycleSilo {
    pub fn open(
        silo_dir: String,
        record_size: usize,
        max_file_size: usize ) -> Result<RecycleSilo, RecordStoreError>
    {
	let data_silo = Silo::open( [silo_dir.clone(),"data".to_string()].join("/"),
				     record_size,
				     max_file_size )?;
	let recycler_silo = Silo::open( [silo_dir.clone(),"recycle".to_string()].join("/"),
					 mem::size_of::<SiloIdData>().try_into().unwrap(),
					 max_file_size )?;
	Ok( RecycleSilo {
	    data_silo,
	    recycler_silo,
	} )
    }

    pub fn live_records(&mut self) -> usize {
        self.data_silo.current_count - self.recycler_silo.current_count
    }

    pub fn recycle_count(&mut self) -> usize {
        self.recycler_silo.current_count
    }

    pub fn data_count(&mut self) -> usize {
        self.data_silo.current_count
    }

    pub fn push(&mut self, record: &SiloByteData) -> Result<usize,RecordStoreError> {
        match self.recycler_silo.pop() {
            Some(data) => {
                self.data_silo.put_record( data.id, record)?;
                Ok(data.id)
            },
            None => self.data_silo.push( record ),
        }
    }

    pub fn put_record(&mut self, id: usize, record: &SiloByteData) -> Result<usize,RecordStoreError> {
        match self.recycler_silo.pop() {
            Some(data) => {
                self.data_silo.put_record( data.id, record )?;
                Ok(data.id)
            },
            None => {
                let new_idx = self.data_silo.push( record )?;
                let _ = self.recycler_silo.push( &SiloIdData::new( id ))?;
                Ok(new_idx)
            }
        }

    }

    pub fn fetch_record(&mut self, idx: usize) -> Option<Vec<u8>> {
        match self.data_silo.fetch_record( idx ) {
            Some(data) => Some(data.bytes.to_vec()),
            None => None
        }
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        match self.data_silo.pop() {
            Some(data) => Some(data.bytes.to_vec()),
            None => None
        }
    }

    pub fn peek(&mut self) -> Option<Vec<u8>> {
        match self.data_silo.peek() {
            Some(data) => Some(data.bytes.to_vec()),
            None => None
        }
    }

}

// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

pub fn silo_files_in_directory(dir: &String) -> Result<Vec<File>, RecordStoreError> {
    //
    // put numeric directory entries in a vec
    //
    let mut dir_entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
                .is_some()
        })
        .collect();

    //
    // sort the dir entries numerically
    //
    dir_entries.sort_by_key(|entry| {
        entry.file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .unwrap_or(u32::MAX)
    });

    //
    // create a vec of File based on those dir entries and "return" it
    //
    let mut silo_files: Vec<File> = Vec::new();
    for entry in &dir_entries {
        silo_files.push( OpenOptions::new()
                         .read(true)
                         .write(true)
                         .create(true)
                         .open(entry.path())?
        );
    }
    Ok( silo_files )
}

pub fn ensure_path(dir: &String) -> Result<(), RecordStoreError> {
    if ! Path::new(&dir).exists() {
        fs::create_dir(dir)?;
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn recycler_silo() {
        let record_size = 64*2;
        let max_file_size = 3 * record_size; // 3 records per file
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();
        eprintln!( "Open {testdir_path}" );
        let mut rsilo = RecycleSilo::open( testdir_path.clone(), record_size, max_file_size )
            .expect("could not open recycle silo");

        assert_eq!(rsilo.recycle_count(),0);
        assert_eq!(rsilo.data_count(),0);

        match rsilo.fetch_record(0) {
            Some(_silo_bytes) => panic!("should be nothing to get"),
            None => assert_eq!( 0, 0 ),
        }
        match rsilo.pop() {
            Some(_silo_bytes) => panic!("pop should return nothing"),
            None => assert_eq!( 0, 0 ),
        }
        match rsilo.peek() {
            Some(_silo_bytes) => panic!("should be nothing to peek at"),
            None => assert_eq!( 0, 0 ),
        }

        let data: [u8; 5] = [0xCA, 0xFE, 0xBA, 0xBE, 0xEE];
        let record = SiloByteData::new( &data );
        let idx = rsilo.push( &record ).ok().unwrap();
        assert_eq!(idx,0);
        assert_eq!(rsilo.recycle_count(),0);
        assert_eq!(rsilo.data_count(),1);
        assert_eq!(rsilo.live_records(),1);


        let data: [u8; 6] = [0x01, 0x01, 0x02, 0x03, 0x05, 0x06];
        let record = SiloByteData::new( &data );
        let idx = rsilo.put_record( 0, &record ).ok().unwrap();
        assert_eq!(idx,1);
        assert_eq!(rsilo.recycle_count(),1);
        assert_eq!(rsilo.data_count(),2);
        assert_eq!(rsilo.live_records(),1);
    }

    #[test]
    fn plain_silo() {
        let record_size = 64*2;
        let max_file_size = 3 * record_size; // 3 records per file
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();
        eprintln!( "Open {testdir_path}" );

        let data: [u8; 6] = [0x01, 0x01, 0x02, 0x03, 0x05, 0x06];
        let more_bytes = SiloByteData::new(&data);

        let data: [u8; 5] = [0xCA, 0xFE, 0xBA, 0xBE, 0xEE];
        let my_bytes = SiloByteData::new( &data );

        let mut silo = Silo::<SiloByteData>::open( testdir_path.clone(), record_size, max_file_size )
            .expect("could not open silo");

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
                assert_eq!( subsilo_md.len(), record_size );
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
                assert_eq!( subsilo_md.len(), record_size * 2 );
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
        let mut silo = Silo::<SiloByteData>::open( testdir_path.clone(), record_size, max_file_size )
            .expect("could not open silo");

        assert_eq!(silo.current_count, 2);
        assert_eq!(silo.subsilos.len(), 1);
        match silo.subsilos.get(0).unwrap().metadata() {
            Ok(subsilo_md) => {
                assert_eq!( subsilo_md.len(), record_size * 2 );
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
                assert_eq!( subsilo_md.len(), record_size * 1 );
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

    }
}
