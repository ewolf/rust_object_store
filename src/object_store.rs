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
use recordstore_macros::init_objects;

use std::collections::HashMap;

use std::sync::{Mutex, OnceLock, Arc};

init_objects!();

#[derive(Serialize, Deserialize, Debug)]
pub struct Reference {
    pub id: u64,
}

impl Reference {
    pub fn new(id: u64) -> Self {
        Reference { id }
    }
    pub fn load<T: ObjectType>(&self, object_store: &mut ObjectStore) -> Result<Box<Obj<T>>, RecordStoreError> {
        object_store.fetch( self.id )
    }
}

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
    Reference(Reference),
}

pub trait ObjectType {
    fn as_any(&self) -> &dyn Any; // Allows downcasting if needed
    fn to_bytes(&self) -> Vec<u8>;

    // was object type factory?
    fn name() -> String;
    fn create_from_bytes(bytes: &[u8]) -> Box<Self>;
}
// Separate trait for serialization/deserialization
pub trait Serializable: Serialize + for<'de> Deserialize<'de> {}

pub struct Obj<T: ObjectType> {
    pub id: u64,

    pub saved: bool,   // true if this object has ever been saved to the data store
    pub dirty: bool,   // true if this object needs to be saved to the data store

    pub data: Box<T>,
}

impl<T: ObjectType> Obj<T> {
    // Creates an Obj with any ObjectType implementation
    pub fn new(data_obj: T) -> Self {
        Obj { 
            id: 0,
            saved: false,
            dirty: true,
            data: Box::new(data_obj),
        }
    }

    // Creates an Obj with any ObjectType implementation
    pub fn new_from_boxed(data_boxed: Box<T>) -> Self {
        Obj { 
            id: 0,
            saved: false,
            dirty: true,
            data: data_boxed,
        }
    }

    // Creates an Obj from bytes
    pub fn from_bytes(bytes: &[u8], id: u64) -> Self {
        Obj::<T> { id, saved: false, dirty: true, data: T::create_from_bytes(bytes) }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.to_bytes()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SaveWrapper<'a> {
    name: &'a str,
    bytes: &'a [u8],
}

static RECORD_STORES: OnceLock<Mutex<HashMap<String,Arc<Mutex<RecordStore>>>>> = OnceLock::new();

fn get_record_store( dir: &str ) -> Arc<Mutex<RecordStore>> {
    let mutex = RECORD_STORES.get_or_init(|| {
        Mutex::new(HashMap::new())
    });
    let mut map = mutex.lock().unwrap();
    map.entry( dir.to_string() )
        .or_insert_with(|| Arc::new(Mutex::new(RecordStore::new(dir))))
        .clone()
}


pub struct ObjectStore {
    base_dir: String,
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
        ObjectStore { base_dir: base_dir.to_string() }
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
    pub fn new_obj<T: ObjectType>(& self, data_obj: T) -> Result<Box<Obj<T>>,RecordStoreError> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        if record_store.current_count() == 0 {
            return Err( RecordStoreError::ObjectStore("new_obj may not be called before root is created".to_string()) );
        }
        let new_id = record_store.next_id()?;
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
    pub fn save_obj<T: ObjectType>(&self,obj: &Obj<T>) -> Result<(),RecordStoreError> {
        let serialized_bytes = obj.to_bytes();
        let wrapper = SaveWrapper {
            name: &T::name(),
            bytes: &serialized_bytes,
        };
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        let serialized_bytes = bincode::serialize(&wrapper).unwrap();
//        println!( "Got bytes {}", serialized_bytes.to_vec().len() );
        record_store.stow( obj.id as usize, &serialized_bytes )?;
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
    pub fn fetch<T: ObjectType>(&self, id: u64) -> Result<Box<Obj<T>>, RecordStoreError> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        self._fetch( &mut record_store, id )
    }

    pub fn _fetch<T: ObjectType>(&self, record_store:&mut RecordStore, id: u64) -> Result<Box<Obj<T>>, RecordStoreError> {
        let bytes = record_store.fetch( id as usize )?.unwrap();
//println!("fetch {} got {} bytes", id, bytes.to_vec().len());
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
    pub fn fetch_root(&self) -> Box<Obj<HashMapObjectType>> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        //        println!( "current count is {}", record_store.current_count() );
        if record_store.current_count() == 0 {
            let _ = record_store.next_id().expect("unable to create root id");
            let new_root = Obj::new(HashMapObjectType::new());
            // id is already 0
            self.save_obj::<HashMapObjectType>(&new_root).expect("unable to save the root");

            return Box::new(new_root)
        }
        eprintln!( "getting" );
        let hmot: Box<HashMapObjectType> = self._fetch::<HashMapObjectType>(&mut record_store,0).expect("unable to fetch the root").data as Box<HashMapObjectType>;
        eprintln!( "got hmot" );
        let root = Obj::new_from_boxed(hmot);
                eprintln!( "got root finally" );
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

#[derive(Serialize, Deserialize, Debug)]
pub struct VecObjectType {
    vec: Vec<ObjectTypeOption>,
}
impl Serializable for VecObjectType {}
impl VecObjectType {
    pub fn new() -> Self {
        VecObjectType {
            vec: Vec::new()
        }
    }
}
pub trait VecObjectTypeExt {
    fn get(&self, key: usize) -> Option<&ObjectTypeOption>;
    fn put(&mut self, key: usize, val: ObjectTypeOption);
}

impl VecObjectTypeExt for Obj<VecObjectType> {
    fn get(&self, key: usize) -> Option<&ObjectTypeOption> {
        self.data.vec.get(key)
    }
    fn put(&mut self, key: usize, val: ObjectTypeOption) {
        self.data.vec.insert(key, val);
    }
}
impl ObjectType for VecObjectType {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to Serialize")
    }

    fn name() -> String { "VecObjectType".to_string() }

    fn create_from_bytes(bytes: &[u8]) -> Box<Self> {
        let deserialized: Self = bincode::deserialize(bytes).expect("Failed to deserialize");
        Box::new(deserialized)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HashMapObjectType {
    pub hashmap: HashMap<String,ObjectTypeOption>
}
impl Serializable for HashMapObjectType {}
pub trait HashMapObjectTypeExt {
    fn get(&self, key: &str) -> Option<&ObjectTypeOption>;
    fn put(&mut self, key: &str, val: ObjectTypeOption);
}
impl HashMapObjectType {
    pub fn new() -> Self {
        HashMapObjectType {
            hashmap: HashMap::new()
        }
    }
}
impl HashMapObjectTypeExt for Box<Obj<HashMapObjectType>> {
    fn get(&self, key: &str) -> Option<&ObjectTypeOption> {
        self.data.hashmap.get(key)
    }
    fn put(&mut self, key: &str, val: ObjectTypeOption) {
        self.data.hashmap.insert(key.to_string(), val);
    }
}
impl ObjectType for HashMapObjectType {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to Serialize")
    }

    fn name() -> String { "HashMapObjectType".to_string() }

    fn create_from_bytes(bytes: &[u8]) -> Box<Self> {
        let deserialized: Self = bincode::deserialize(bytes).expect("Failed to deserialize");
        Box::new(deserialized)
    }
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
