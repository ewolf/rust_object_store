/*
 TODO:
    
  * start with writing the driver and learn what is gonna be
    needed in the object_store api

  * make sql version of the record store

  *  write tests with the Getters macro

  *  rename Getters macro to Obj or something

  * Apply getters and setters to Obj itself? 
      can dyn ObjType be the Obj type?

  * Make a reference Option for connecting different objects

  * HashMap and Vec implementations of ObjType
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
pub struct Ref {
    pub id: u64,
}

impl Ref {
    pub fn new(id: u64) -> Self {
        Ref { id }
    }
    pub fn load<T: ObjType>(&self, object_store: &mut ObjectStore) -> Result<Box<Obj<T>>, RecordStoreError> {
        object_store.fetch( self.id )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ObjTypeOption {
    Bool(bool),
    Char(char),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    String(String),
    Ref(Ref),
}

pub trait ObjType {
    fn as_any(&self) -> &dyn Any; // Allows downcasting if needed
    fn to_bytes(&self) -> Vec<u8>;

    // was object type factory?
    fn name() -> String;
    fn create_from_bytes(bytes: &[u8]) -> Box<Self>;
}

// Separate trait for serialization/deserialization
pub trait Serializable: Serialize + for<'de> Deserialize<'de> {}

pub struct Obj<T: ObjType> {
    pub id: u64,

    pub saved: bool,   // true if this object has ever been saved to the data store
    pub dirty: bool,   // true if this object needs to be saved to the data store

    pub data: Box<T>,
}

impl<T: ObjType> Obj<T> {
    // Creates an Obj with any ObjType implementation
    pub fn new(data_obj: T) -> Self {
        Obj { 
            id: 0,
            saved: false,
            dirty: true,
            data: Box::new(data_obj),
        }
    }

    // Creates an Obj with any ObjType implementation
    pub fn new_from_boxed(data_boxed: Box<T>) -> Self {
        Obj { 
            id: 0,
            saved: false,
            dirty: true,
            data: data_boxed,
        }
    }

    pub fn make_ref(&self) -> Ref {
        Ref { id: self.id }
    }

    pub fn make_ref_opt(&self) -> ObjTypeOption {
        ObjTypeOption::Ref(Ref { id: self.id })
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

/// Returns a thread safe mutex holding a record store for the given directory. 
/// Each record store lives in its own directory.
///
/// # Arguments
/// 
/// * dir - directory where record store lives.
///
/// # Returns
///
/// * Arc<Mutex<RecordStore>>
///
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

    pub fn record_store_binding(&self) -> Arc<Mutex<RecordStore>> {
        get_record_store( &self.base_dir )
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
    pub fn new_obj<T: ObjType>(& self, data_obj: T) -> Result<Box<Obj<T>>,RecordStoreError> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        self.new_obj_rs( &mut record_store, data_obj)
    }

    pub fn new_obj_rs<T: ObjType>(& self, record_store: &mut RecordStore, data_obj: T) -> Result<Box<Obj<T>>,RecordStoreError> {
        if record_store.current_count() == 0 {
            return Err( RecordStoreError::ObjectStore("new_obj may not be called before root is created".to_string()) );
        }
        let new_id = record_store.next_id()?;
        let mut new_obj = Obj::<T>::new(data_obj);
        new_obj.id = new_id as u64;

        self.save_obj_rs( record_store, &mut new_obj )?;

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
    pub fn save_obj<T: ObjType>(&self,obj: &mut Obj<T>) -> Result<(),RecordStoreError> {
        if obj.dirty {
            let binding = get_record_store( &self.base_dir );
            let mut record_store = binding.lock().unwrap();
            let _ = self.save_obj_rs( &mut record_store, obj );
        }
        Ok(())
    }

    pub fn save_obj_rs<T: ObjType>(&self, record_store:&mut RecordStore, obj: &mut Obj<T>) -> Result<(),RecordStoreError> {
        if obj.dirty {
            let serialized_bytes = obj.to_bytes();
            let wrapper = SaveWrapper {
                name: &T::name(),
                bytes: &serialized_bytes,
            };
            let serialized_bytes = bincode::serialize(&wrapper).unwrap();
//            eprintln!( "SAVE ID {}, Got bytes {}", obj.id, serialized_bytes.to_vec().len() );
            record_store.stow( obj.id as usize, &serialized_bytes )?;
            obj.dirty = false;
//        } else {
//            eprintln!( "NOT DIRTY ID {}", obj.id );
        }
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
    pub fn fetch<T: ObjType>(&self, id: u64) -> Result<Box<Obj<T>>, RecordStoreError> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        self.fetch_rs( &mut record_store, id )
    }

    pub fn fetch_rs<T: ObjType>(&self, record_store:&mut RecordStore, id: u64) -> Result<Box<Obj<T>>, RecordStoreError> {
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
    pub fn fetch_root(&self) -> Box<Obj<HashMapObjType>> {
        let binding = get_record_store( &self.base_dir );
        let mut record_store = binding.lock().unwrap();
        self.fetch_root_rs( &mut record_store )
    }

    pub fn fetch_root_rs(&self, record_store: &mut RecordStore) -> Box<Obj<HashMapObjType>> {
        if record_store.current_count() == 0 {
            let _ = record_store.next_id().expect("unable to create root id");
            let mut new_root = Obj::new(HashMapObjType::new());
            // id is already 0
            self.save_obj_rs::<HashMapObjType>( record_store, &mut new_root).expect("unable to save the root");

            return Box::new(new_root)
        }
        let hmot: Box<HashMapObjType> = self.fetch_rs::<HashMapObjType>(record_store,0).expect("unable to fetch the root").data as Box<HashMapObjType>;
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

//---------------------------------------------------------------------------------
//---------------------------------------------------------------------------------
//---------------------------------------------------------------------------------
//---------------------------------------------------------------------------------
/*

#[derive(Serialize, Deserialize, Debug)]
pub struct VecObjType<T> {
    vec: Vec<T>,
}
impl VecObjType {
    pub fn new() -> Self {
        VecObjType {
            vec: Vec::new()
        }
    }
}
pub trait VecObjTypeExt {
    fn get(&self, key: usize) -> Option<&ObjTypeOption>;
    fn len(&self) -> usize;
    fn push(&mut self, val: ObjTypeOption);
    fn insert(&mut self, key: usize, val: ObjTypeOption);
}

impl VecObjTypeExt for Obj<VecObjType> {
    fn get(&self, key: usize) -> Option<&ObjTypeOption> {
        self.data.vec.get(key)
    }
    fn push(&mut self, val: ObjTypeOption) {
        self.dirty = true;
        self.data.vec.push(val);
    }
    fn len(&self) -> usize {
        self.data.vec.len()
    }
    fn insert(&mut self, key: usize, val: ObjTypeOption) {
        self.dirty = true;
        self.data.vec.insert(key, val);
    }
}
impl ObjType for VecObjType {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to Serialize")
    }

    fn name() -> String { "VecObjType".to_string() }

    fn create_from_bytes(bytes: &[u8]) -> Box<Self> {
        let deserialized: Self = bincode::deserialize(bytes).expect("Failed to deserialize");
        Box::new(deserialized)
    }
}
*/

//---------------------------------------------------------------------------------
//---------------------------------------------------------------------------------
//---------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug)]
pub struct HashMapObjType {
    pub hashmap: HashMap<String,ObjTypeOption>
}
impl Serializable for HashMapObjType {}
pub trait HashMapObjTypeExt {
    fn get(&self, key: &str) -> Option<&ObjTypeOption>;
    fn get_default(&mut self, key: &str, default: &dyn Fn() -> ObjTypeOption) -> &ObjTypeOption;
    fn put(&mut self, key: &str, val: ObjTypeOption);
}
impl HashMapObjType {
    pub fn new() -> Self {
        HashMapObjType {
            hashmap: HashMap::new()
        }
    }
}
impl HashMapObjTypeExt for Box<Obj<HashMapObjType>> {
    fn get(&self, key: &str) -> Option<&ObjTypeOption> {
        self.data.hashmap.get(key)
    }
    fn get_default(&mut self, key: &str, default: &dyn Fn() -> ObjTypeOption ) -> &ObjTypeOption {
        self.data.hashmap.entry(key.to_string()).or_insert_with( || { self.dirty = true; default() } )
    }
    fn put(&mut self, key: &str, val: ObjTypeOption) {
        self.dirty = true;
        self.data.hashmap.insert(key.to_string(), val);
    }
}
impl ObjType for HashMapObjType {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to Serialize")
    }

    fn name() -> String { "HashMapObjType".to_string() }

    fn create_from_bytes(bytes: &[u8]) -> Box<Self> {
        let deserialized: Self = bincode::deserialize(bytes).expect("Failed to deserialize");
        Box::new(deserialized)
    }
}


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

    impl ObjType for Canary {
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

    impl ObjType for Root {
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
