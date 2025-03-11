//!
//! 
//!
//!
use crate::record_store::RecordStore;
use crate::silo::RecordStoreError;
use serde::{Serialize, Deserialize};
use bincode;


//use std::sync::{Arc, Mutex};
//use std::collections::HashMap;
//use lazy_static::lazy_static;
/*
lazy_static! {
    static ref DIRTY_CACHE: Arc<Mutex<HashMap<u64, Box<dyn Fn() -> Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
}

trait Saveable: Send + Sync {
    
}
*/

//static DIRTY_CACHE: Arc<Mutex<HashMap<u64, Box<Obj<dyn Saveable>>>>> = Arc::new(Mutex::new(HashMap::new()));

pub trait ObjectType: Serialize + for<'de> Deserialize<'de> {
    fn new() -> Box<Self>;
    fn load(bytes: &[u8]) -> Result<Box<Self>,RecordStoreError>;
//    fn bytes() -> Vec<u8>;
    fn name() -> String;
}

///
/// An Obj manages data for a particular type and is connected to the store.
/// 
pub struct Obj<T> {
    /// The id assigned by the ObjectStore.
    pub id: u64,

    saved: bool,   // true if this object has ever been saved to the data store
    dirty: bool,   // true if this object needs to be saved to the data store
    data: Box<T>,  // object 
}

impl<T: ObjectType> Obj<T> {

    ///
    /// Returns a new empty object with 
    /// default type data.
    ///
    pub fn new() -> Obj<T> {
        Obj {
            id: 0,
            saved: false,
            dirty: true,
            data: T::new(),
        }
    }

    ///
    /// Mark the data in this object as needing a save.
    ///
    pub fn dirty(&self) {
        //let DIRTY_CACHE: Arc<Mutex<HashMap<u64, Box<Obj<dyn Saveable>>>>> = Arc::new(Mutex::new(HashMap::new()));
        //        let mut cache = DIRTY_CACHE.lock().unwrap();
        //        cache.insert( self.id, Box::new(|| bincode::serialize(&self.data).unwrap().to_vec()) );
    }
    
    /// Construct an object given an array of bytes.
    ///
    /// # Arguments
    ///
    /// * bytes - an array of bytes.
    ///
    /// # Returns
    ///
    /// * Ok(Obj<T>) - the constructed object
    /// * Err(RecordStoreError::ObjectStore) - the bytes indicate a different data type than the Obj's type.
    ///
    pub fn load(bytes: &[u8]) -> Result<Obj<T>,RecordStoreError> {
        let data_obj = T::load(bytes)?;
        Ok(Obj {
            id: 0,
            saved: true,
            dirty: false,
            data: data_obj,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SaveWrapper<'a> {
    name: &'a str,
    bytes: &'a [u8],
}

impl SaveWrapper<'_> {
    
}

pub struct ObjectStore {
    record_store: RecordStore,
}

impl ObjectStore {

    /// Creates new ObjectStore for the given directory.
    ///
    /// # Arguments
    ///
    /// * base_dir - directory to store the object store files.
    ///
    /// # Returns
    ///
    /// * ObjectStore
    ///
    pub fn new(base_dir: &str) -> ObjectStore {
        let record_store = RecordStore::new( base_dir );

        let os = ObjectStore {
            record_store,
        };

        os
    }
    ///
    /// Create a new Obj of the given type
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn new_obj<T: ObjectType>(&mut self) -> Result<Box<Obj<T>>,RecordStoreError> {
        if self.record_store.current_count() == 0 {
            return Err( RecordStoreError::ObjectStore("new_obj may not be called before root is created".to_string()) );
        }
        let new_id = self.record_store.next_id()?;
        let mut new_obj = Obj::<T>::new();
        new_obj.id = new_id as u64;
        //  register it here or whatnot
        Ok(Box::new(new_obj))
    }

    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn save_obj<T: ObjectType>(&mut self,obj: &Obj<T>) -> Result<(),RecordStoreError> {
        let data = &obj.data;
        let serialized_bytes = bincode::serialize(&data).unwrap();
        let wrapper = SaveWrapper {
            name: &T::name(),
            bytes: &serialized_bytes,
        };
        let serialized_bytes = bincode::serialize(&wrapper).unwrap();
        self.record_store.stow( obj.id as usize, &serialized_bytes )?;
        Ok(())
    }

    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn fetch<T: ObjectType>(&mut self, id: usize) -> Result<Box<Obj<T>>, RecordStoreError> {
        let bytes = self.record_store.fetch( id )?.unwrap();
        let obj = Obj::<T>::load(&bytes)?;
        Ok(Box::new(obj))
    }
    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn fetch_root<T: ObjectType>(&mut self) -> Result<Box<Obj<T>>,RecordStoreError> {
        if self.record_store.current_count() == 0 {
            let _ = self.record_store.next_id()?;
            let new_root = Obj::<T>::new();
            self.save_obj(&new_root)?;

            return Ok(Box::new(new_root));
        }
        Ok(self.fetch(0)?)
    }
/*
    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn save(&mut self) {

    }
    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn fetch_path(&mut self, path: &str) {

    }
    ///
    ///
    /// # Arguments
    ///
    ///
    ///
    /// # Returns
    ///
    ///
    ///
    pub fn ensure_path(&mut self, path: &str) {

    }
*/
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Canary {
        is_alive: bool,
        wingspan: u64,
    }

    impl ObjectType for Canary {
        fn name() -> String {
            "canary".to_string()
        }
        fn new() -> Box<Canary> {
            Box::new(Canary { is_alive: true, wingspan: 3 })
        }
//        fn bytes() -> Vec<u8> {}
        fn load(bytes: &[u8]) -> Result<Box<Canary>,RecordStoreError> {
            let wrapper: SaveWrapper = bincode::deserialize(bytes).expect("canary load failed");
            if wrapper.name != "canary" {
                return Err( RecordStoreError::ObjectStore("Error, tried to load canary and got {wrapper.name}".to_string()) )
            }
            let canary: Canary = bincode::deserialize(wrapper.bytes).expect("canary load failed");
            Ok(Box::new( canary ))
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Root {

    }

    impl ObjectType for Root {
        fn name() -> String {
            "root".to_string()
        }
        fn new() -> Box<Root> {
            Box::new(Root {})
        }
        fn load(bytes: &[u8]) -> Result<Box<Root>,RecordStoreError> {
            let wrapper: SaveWrapper = bincode::deserialize(bytes).expect("root load failed");
            if wrapper.name.to_string() != "root".to_string() {
                return Err( RecordStoreError::ObjectStore("tried to load root and got {wrapper.name}".to_string()) );
            }
            let root: Root = bincode::deserialize(wrapper.bytes).expect("root load failed");
            Ok(Box::new( root ))
        }
    }

    #[test]
    fn object_store() {
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();
        let mut os: ObjectStore = ObjectStore::new( &testdir_path );
        match os.new_obj::<Canary>() {
            Ok(_thing) => panic!("allowed an object to be created before the root was"),
            Err(err) => assert_eq!( err.to_string(), "An object store error occurred: new_obj may not be called before root is created" )
        }

        let root = os.fetch_root::<Root>().unwrap();
        assert_eq!( root.id, 0 );


        let canary = os.new_obj::<Canary>().unwrap();
        assert_eq!( canary.id, 1 );
        assert_eq!( canary.saved, false );
        assert_eq!( canary.dirty, true );
        assert_eq!( canary.data.wingspan, 3 );
    }
}

