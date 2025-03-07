use crate::record_store::RecordStore;
use crate::silo::RecordStoreError;
use serde::{Serialize, Deserialize};
use bincode;
use std::collections::HashMap;
use std::marker::PhantomData;

pub trait ObjectType: Serialize + for<'de> Deserialize<'de> {
    fn new() -> Box<Self>;
    fn load(bytes: &[u8]) -> Box<Self>;
}

pub struct Obj<'a, T, B> {
    id: u64,
    saved: bool,
    dirty: bool,
    data: Box<T>,
    object_store: &'a ObjectStore<B>,
    _object_type: PhantomData<T>,
}

impl<'a, T: ObjectType, B> Obj<'_, T, B> {
    pub fn new(object_store: &'a ObjectStore<B>) -> Obj<'a, T, B> {
        Obj {
            id: 0,
            saved: false,
            dirty: true,
            data: T::new(),
            object_store,
            _object_type: PhantomData,
        }
    }
    pub fn load(object_store: &'a ObjectStore<B>,bytes: &[u8]) -> Obj<'a, T, B> {
        Obj {
            id: 0,
            saved: true,
            dirty: false,
            data: T::load(bytes),
            object_store,
            _object_type: PhantomData,
        }
    }
/*
    pub fn save(&mut self) -> Result<&ObjectStore<B>,RecordStoreError> {
        self.object_store.save_obj(self)?;
        Ok(self.object_store)
    }
*/
}


#[derive(Serialize, Deserialize)]
struct RegistryWrapper<'a> {
    type_name: String,
    bytes: &'a [u8],
}

impl RegistryWrapper<'_> {
    fn new( type_name: String, bytes: &[u8] ) -> RegistryWrapper {
        RegistryWrapper {
            type_name,
            bytes,
        }
    }
}

pub struct ObjectStore<T> {
    record_store: RecordStore,
    _root_object_type: PhantomData<T>,
}

impl<B: ObjectType> ObjectStore<B> {

    pub fn new(base_dir: &str) -> ObjectStore<B> {
        let record_store = RecordStore::new( base_dir );

        ObjectStore::<B> {
            record_store,
            _root_object_type: PhantomData,
        }
    }
    pub fn new_obj<T: ObjectType>(&mut self) -> Result<Box<Obj<T,B>>,RecordStoreError> {
        let new_id = self.record_store.next_id()?;
        if new_id == 0 {
            // needs a root to be installed
            let _ = self.fetch_root()?;
        }
        let new_id = self.record_store.next_id()?;
        let mut new_obj = Obj::<T, B>::new(self);
        new_obj.id = new_id as u64;
        //  register it here or whatnot
        Ok(Box::new(new_obj))
    }

    pub fn save_obj<T: ObjectType>(&mut self,obj: &Obj<T,B>) -> Result<(),RecordStoreError> {
        let data = &obj.data;
        let serialized_bytes = bincode::serialize(&data).unwrap();
        self.record_store.stow( obj.id as usize, &serialized_bytes )?;
        Ok(())
    }

    pub fn fetch<T: ObjectType>(&mut self, id: usize) -> Result<Box<Obj<T,B>>,RecordStoreError> {
        let bytes = self.record_store.fetch( id )?.unwrap();
        let loaded_obj = Obj::<T,B>::load(self,&bytes);
        Ok(Box::new(loaded_obj))
    }
    pub fn fetch_root(&mut self) -> Result<Box<Obj<B,B>>,RecordStoreError> {
        if self.record_store.current_count() == 0 {
            let _ = self.record_store.next_id()?;            
            let mut new_root = Obj::<B, B>::new(self);
//            new_root.save();
            return Ok(Box::new(new_root));
        }
        Ok(self.fetch(0)?)
    }
/*
    pub fn save(&mut self) {

    }
    pub fn fetch_path(&mut self, path: &str) {

    }
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
        fn new() -> Box<Canary> {
            Box::new(Canary { is_alive: true, wingspan: 3 })
        }
        fn load(bytes: &[u8]) -> Box<Canary> {
            let canary: Canary = bincode::deserialize(bytes).expect("canary load failed");
            Box::new( canary )
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Root {
        
    }

    impl ObjectType for Root {
        fn new() -> Box<Root> {
            Box::new(Root {})
        }
        fn load(bytes: &[u8]) -> Box<Root> {
            let root: Root = bincode::deserialize(bytes).expect("root load failed");
            Box::new( root )
        }
    }

    #[test]
    fn object_store() {
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();

        let mut os: ObjectStore<Root> = ObjectStore::new( &testdir_path );
/*
        let canary = os.new_obj::<Canary>();
        assert_eq!( canary.id, 0 );
        assert_eq!( canary.saved, false );
        assert_eq!( canary.dirty, true );
        assert_eq!( canary.data.wingspan, 3 );
*/
    }
}
