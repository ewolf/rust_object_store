/*
 TODO:
    
  * start with writing the driver and learn what is gonna be
    needed in the object_store api

  * make sql version of the record store

  *  write tests with the Getters macro

  *  rename Getters macro to Obj or something

  * Apply getters and setters to Obj itself? 
      can dyn ObjectType be the Obj type?

  * Make a reference Option for connecting different objects

  * HashMap and Vec implementations of ObjectType
     - trying at the bottom of this file

  * Create Root

also if let Some(a) = obj_from_bytes.data.as_any().downcast_ref::<A>()

*/


//!
//! 
//!
//!
use crate::record_store::RecordStore;
use crate::silo::RecordStoreError;
use recordstore_macros::{init_objects,Getters};

use std::collections::HashMap;

init_objects!();

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SaveWrapper<'a> {
    name: &'a str,
    bytes: &'a [u8],
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
    pub fn new_obj<T: ObjectType>(&mut self, data_obj: T) -> Result<Box<Obj<T>>,RecordStoreError> {
        if self.record_store.current_count() == 0 {
            return Err( RecordStoreError::ObjectStore("new_obj may not be called before root is created".to_string()) );
        }
        let new_id = self.record_store.next_id()?;
        let mut new_obj = Obj::<T>::new(data_obj);
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
        let serialized_bytes = obj.to_bytes();
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
    pub fn fetch<T: ObjectType>(&mut self, id: u64) -> Result<Box<Obj<T>>, RecordStoreError> {
        let bytes = self.record_store.fetch( id as usize )?.unwrap();
        let wrapper: SaveWrapper = bincode::deserialize(&bytes)?;
        if wrapper.name != T::name() {
            return Err(RecordStoreError::ObjectStore("Error, expected '{T::Name}' and fetched '{wrapper.name}".to_string()));
        }
        let obj = Obj::from_bytes(&wrapper.bytes, id);
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
    pub fn fetch_root(&mut self) -> Box<Obj<HashMapObjectType>> {
        if self.record_store.current_count() == 0 {
            let _ = self.record_store.next_id().expect("unable to create root id");
            let new_root = Obj::new(HashMapObjectType::new());
            // id is already 0
            self.save_obj::<HashMapObjectType>(&new_root).expect("unable to save the root");

            return Box::new(new_root)
        }
        let hmot: Box<HashMapObjectType> = self.fetch::<HashMapObjectType>(0).expect("unable to fetch the root").data as Box<HashMapObjectType>;
        let root = Obj::new_from_boxed(hmot);
        Box::new(root)
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

/*

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    #[derive(Getters)]
    struct Canary {
        wingspan: i32,
        name: String,
    }
    impl Canary {
        fn new_default() -> Self {
            Canary { wingspan: 12, name: "Beaux".to_string() }
        }
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

#[derive(Serialize, Deserialize, Debug)]
struct Canary {
    wingspan: i32,
    name: String,
}
*/

#[derive(Serialize, Deserialize, Debug)]
pub enum ObjectTypeOption {
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
}

#[derive(Serialize, Deserialize, Debug, Getters)]
pub struct VecObjectType {
    pub vec: Vec<ObjectTypeOption>
}
impl VecObjectType {
    pub fn new() -> Self {
        VecObjectType {
            vec: Vec::new()
        }
    }
}

impl Obj<VecObjectType> {
    pub fn get(&self, key: usize) -> Option<&ObjectTypeOption> {
        self.data.vec.get(key)
    }
    pub fn put(&mut self, key: usize, val: ObjectTypeOption) {
        self.data.vec.insert(key, val);
    }
}

#[derive(Serialize, Deserialize, Debug, Getters)]
pub struct HashMapObjectType {
    pub hashmap: HashMap<String,ObjectTypeOption>
}
impl HashMapObjectType {
    pub fn new() -> Self {
        HashMapObjectType {
            hashmap: HashMap::new()
        }
    }
}
impl Obj<HashMapObjectType> {
    pub fn get(&self, key: String) -> Option<&ObjectTypeOption> {
        self.data.hashmap.get(&key)
    }
    pub fn put(&mut self, key: String, val: ObjectTypeOption) {
        self.data.hashmap.insert(key, val);
    }
}
