use crate::silo::{Silo, RecordStoreError};

use serde::{Serialize, Deserialize};
use std::mem;
use std::vec::Vec;


// ----------------------------------------------------------------

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

// ----------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SiloIdData {
    id: usize,
}
impl SiloIdData {
    fn new( id: usize ) -> SiloIdData {
         SiloIdData {
            id
        }
    }
}


// ----------------------------------------------------------------

pub struct RecycleSilo {
    data_silo: Silo<SiloByteData>,
    recycler_silo: Silo<SiloIdData>,
}

impl RecycleSilo {
    pub fn new(
        silo_dir: String,
        record_size: usize,
        max_file_size: usize ) -> RecycleSilo {

	let data_silo = Silo::new( [silo_dir.clone(),"data".to_string()].join("/"),
				    record_size,
				    max_file_size );
	let recycler_silo = Silo::new( [silo_dir.clone(),"recycle".to_string()].join("/"),
					mem::size_of::<SiloIdData>().try_into().unwrap(),
					max_file_size );
        RecycleSilo {
	    data_silo,
	    recycler_silo,
	}
    }
    pub fn open(&mut self) -> Result<(), RecordStoreError>
    {
        let _ = self.data_silo.open();
        let _ = self.recycler_silo.open();
        Ok(())
    }

    pub fn recycle(&mut self, idx: usize) -> Result<(),RecordStoreError> {
        self.recycler_silo.push( &SiloIdData::new( idx ))?;
        Ok(())
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
        let mut rsilo = RecycleSilo::new( testdir_path.clone(), record_size, max_file_size );
        rsilo.open().expect("could not open recycle silo");

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

        let fetched_record = rsilo.fetch_record( 0 ).unwrap();
        assert_eq!(fetched_record, data.to_vec() );


        let data: [u8; 6] = [0x01, 0x01, 0x02, 0x03, 0x05, 0x06];
        let record = SiloByteData::new( &data );
        let idx = rsilo.put_record( 0, &record ).ok().unwrap();
        assert_eq!(idx,1);
        assert_eq!(rsilo.recycle_count(),1);
        assert_eq!(rsilo.data_count(),2);
        assert_eq!(rsilo.live_records(),1);

        let fetched_record = rsilo.fetch_record( 1 ).unwrap();
        assert_eq!(fetched_record, data.to_vec() );
    }
}
