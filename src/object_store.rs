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

pub struct Obj<T> {
    id: u64,
    saved: bool,
    dirty: bool,
    data: Box<T>,
    _object_type: PhantomData<T>,
}

impl<T: ObjectType> Obj<T> {
    pub fn new() -> Obj<T> {
        Obj {
            id: 0,
            saved: false,
            dirty: true,
            data: T::new(),
            _object_type: PhantomData,
        }
    }
    pub fn load(bytes: &[u8]) -> Obj<T> {
        Obj {
            id: 0,
            saved: true,
            dirty: false,
            data: T::load(bytes),
            _object_type: PhantomData,
        }
    }
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

pub struct ObjectStore {
    record_store: RecordStore,
}

impl ObjectStore {

    pub fn new(base_dir: &str) -> ObjectStore {
        let record_store = RecordStore::new( base_dir );

        ObjectStore {
            record_store,
        }
    }

    pub fn new_obj<T: ObjectType>(&mut self) -> Box<Obj<T>> {
        let new_obj = Obj::<T>::new();
        //  register it here or whatnot
        Box::new(new_obj)
    }

    pub fn save_obj<T: ObjectType>(&mut self,obj: &Obj<T>) -> Result<(),RecordStoreError> {
        let data = &obj.data;
        let serialized_bytes = bincode::serialize(&data).unwrap();
        self.record_store.stow( obj.id as usize, &serialized_bytes )?;
        Ok(())
    }

    pub fn fetch<T: ObjectType>(&mut self, id: usize) -> Result<Box<Obj<T>>,RecordStoreError> {
        let bytes = self.record_store.fetch( id )?.unwrap();
        let loaded_obj = Obj::<T>::load(&bytes);
        Ok(Box::new(loaded_obj))
    }

/*
    pub fn fetch_root(&mut self) {

    }
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


    #[test]
    fn object_store() {
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();

        let mut os = ObjectStore::new( &testdir_path );
/*
        let _ = os.object_factory.register( "canary", 
                                             || { let obj:Obj<Canary> = Obj::new(); 
                                                  obj.data = Some(Canary {wingspan:0});
                                                  Box::new(obj)
                                             },
                                             |bytes| { let canary: Canary = bincode::deserialize(bytes).expect("GOOGA");
                                                       let obj:Obj<Canary> = Obj::new(); 
                                                       obj.data = Some(canary);
                                                       obj.dirty = false;
                                                       obj.saved = true;
                                                       Box::new(obj)
                                             } );

        match os.new_obj("canary".to_string()) {
            Ok(canary_box) => {
                let canary = canary_box;
                assert_eq!( canary.id, 0 );
                assert_eq!( canary.saved, false );
                assert_eq!( canary.dirty, true );
                match canary.data {
                    None => panic!("no canary"),
                    Some( data ) => {
                        let cdata: Canary = data as Canary;
                        assert_eq!( cdata.wingspan, 0 );
                    }
                }
            }
            Err(_err) => panic!("canary died")
        }
         */        
    }
}
