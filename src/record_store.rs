use crate::silo::{Silo, RecordStoreError};
use crate::recycle_silo::{RecycleSilo, SiloByteData};

use serde::{Serialize, Deserialize};
use std::mem;
use std::vec::Vec;

const RECORD_QUANTA   : usize = 4_096;  // records must be multiples of this
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

// ------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct RecordIndexData {
    silo_idx: usize,
    idx_in_silo: usize,
    // add timestamp here?
}
impl RecordIndexData {
    fn new( silo_idx: usize, idx_in_silo: usize ) -> RecordIndexData {
        RecordIndexData {
            silo_idx,
            idx_in_silo,
        }
    }
}

// ------------------------------------------------------------------------------------------------

pub struct RecordStore {
    data_silos: Vec<RecycleSilo>,
    index_silo: Silo<RecordIndexData>,
}


impl RecordStore {

    pub fn new(base_dir: &str) -> RecordStore {
        let mut data_silos: Vec<RecycleSilo> = Vec::new();
        let mut silo_id = 0;
        data_silos.resize_with(MAX_SILO_ID as usize + 1,
                               move || {
                                   silo_id += 1;
                                   RecycleSilo::new( [base_dir,"data_silos",&silo_id.to_string()].join("/"),
                                                      size_for_silo_id(silo_id),
                                                      MAX_FILE_SIZE
                                   )
                               });
        let index_silo = Silo::new( [base_dir,"data_index"].join("/"),
                                     mem::size_of::<RecordIndexData>().try_into().unwrap(),
                                     MAX_FILE_SIZE );
        RecordStore {
            data_silos,
            index_silo,
        }
    }

    pub fn next_id(&mut self) -> Result<usize,RecordStoreError> {
        let _ = self.index_silo.open();

        let data_id = self.index_silo.current_count;

        let index_data = RecordIndexData::new( 0, 0 );
        let _ = self.index_silo.push( &index_data )?;

        Ok( data_id )
    }

    pub fn fetch(&mut self, id: usize) -> Result<Option<Vec<u8>>,RecordStoreError> {
        let _ = self.index_silo.open();
        let index_record = self.index_silo.fetch_record( id ).ok_or(RecordStoreError::RecordStore("stow: unable to find index record for {id}".to_string()))?;
        let silo_idx = index_record.silo_idx;
        let idx_in_silo = index_record.idx_in_silo;
        if let Some(data_silo) = self.data_silos.get_mut(silo_idx) {
            let _ = data_silo.open();
            return Ok( data_silo.fetch_record(idx_in_silo) )
        } else {
            return Err(RecordStoreError::RecordStore("fetch: unable to find silo for index {silo_idx}".to_string()));
        }
    }

    pub fn stow(&mut self, id: usize, data: &[u8]) -> Result<(),RecordStoreError> {
        let _ = self.index_silo.open();

        let index_record = self.index_silo.fetch_record( id ).ok_or(RecordStoreError::RecordStore("stow: unable to find index record for {id}".to_string()))?;

        let old_silo_idx = index_record.silo_idx;
        let old_idx_in_silo = index_record.idx_in_silo;

        let record = SiloByteData::new( &data );
        let new_silo_idx = silo_id_for_size( record.size() );

        if let Some(new_data_silo) = self.data_silos.get_mut(new_silo_idx) {
            let _ = new_data_silo.open();
            let new_idx_in_silo = new_data_silo.push(&record)?;
            let index_data = RecordIndexData::new( new_silo_idx, new_idx_in_silo );
            self.index_silo.put_record( id, &index_data )?;
        } else {
            return Err(RecordStoreError::RecordStore("stow: unable to find silo for index {new_silo_idx}".to_string()));
        }

        if let Some(old_data_silo) = self.data_silos.get_mut(old_silo_idx) {
            let _ = old_data_silo.open();
            old_data_silo.recycle(old_idx_in_silo)?;
        } else {
            return Err(RecordStoreError::RecordStore("stow: unable to find silo for index {old_silo_idx}".to_string()));
        }

        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    #[test]
    fn record_store() {
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();
        let mut rs = RecordStore::new( &testdir_path );
        match rs.fetch(0) {
            Err(_err) => assert_eq!( 1, 1 ),
            Ok(_record) => panic!("fetch returns something in empty rs")
        }

        let data: [u8; 5] = [0xCA, 0xFE, 0xBA, 0xBE, 0xEE];

        match rs.stow(0, &data) {
            Err(_err) => assert_eq!( 1, 1 ),
            Ok(()) => panic!("fetch returns something in empty rs")
        }

        let new_id = rs.next_id().unwrap();
        assert_eq!( new_id, 0 );

        let _ = rs.stow( new_id, &data);
        let fetch_data = rs.fetch(0).ok().unwrap().unwrap();
        assert_eq!( fetch_data, data.to_vec() );

        let new_data: [u8; 5] = [0xCA, 0xFE, 0xBA, 0xBE, 0xEE];

        match rs.stow(0, &new_data) {
            Err(err) => panic!("{}", err),
            Ok(()) => assert_eq!( 1, 1 )
        }

        let fetch_data = rs.fetch(0).ok().unwrap().unwrap();
        assert_eq!( fetch_data, new_data.to_vec() );

    }
}
