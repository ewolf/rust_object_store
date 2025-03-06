use crate::record_store::RecordStore;
use crate::silo::RecordStoreError;
use erased_serde::{serialize_trait_object, Serialize as ErasedSerialize};
use serde::{Serialize, Deserialize};
use bincode;
use std::collections::HashMap;
use std::marker::PhantomData;

type FactoryNew = fn() -> Box<Obj<dyn ObjectType>>;
type FactoryDeserialize = fn(&[u8]) -> Box<Obj<dyn ObjectType>>;
pub struct TypeRegistry {
    new_map: HashMap<String, FactoryNew>,
    deserialize_map: HashMap<String, FactoryDeserialize>,
}
impl TypeRegistry {
    fn new() -> TypeRegistry {
        TypeRegistry {
            new_map: HashMap::new(),
            deserialize_map: HashMap::new(),
        }
    }
    pub fn register(&mut self, 
                    name: &str, 
                    new_fn: FactoryNew,
                    deserialize_fn: FactoryDeserialize) 
                    -> Result<(),RecordStoreError> {
        match self.new_map.get(name) {
            None => {
                self.new_map.insert( name.to_string(), new_fn );
                self.deserialize_map.insert( name.to_string(), deserialize_fn );
                Ok(())
            },
            Some(_fn) => Err(RecordStoreError::ObjectStore(String::from("Error. object type {name} already registered")))
        }
    }
    fn new_obj(&mut self, name: &str) -> Result<Box<Obj<dyn ObjectType>>,RecordStoreError> {
        match self.new_map.get(name) {
            None => Err(RecordStoreError::ObjectStore(String::from("Error. object type {name} not found"))),
            Some(new_fn) => Ok(new_fn())
        }
    }
    fn deserialize_obj(&mut self, name: &str, bytes: &[u8]) -> Result<Box<Obj<dyn ObjectType>>,RecordStoreError> {
        match self.deserialize_map.get(name) {
            None => Err(RecordStoreError::ObjectStore(String::from("Error. object type {name} not found"))),
            Some(d_fn) => Ok(d_fn(bytes))
        }
    }
}


pub trait ObjectType : ObjectTypeClone + ErasedSerialize {// +  + Deserialize {
    fn name(&self) -> String;
}

// Implement serialization for the trait
serialize_trait_object!(ObjectType);

pub trait ObjectTypeClone {
    fn clone_box(&self) -> Box<dyn ObjectType>;
}

impl<T: 'static + ObjectType + Clone>ObjectTypeClone for T {
    fn clone_box(&self) -> Box<dyn ObjectType> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ObjectType> {
    fn clone(&self) -> Box<dyn ObjectType> {
        self.clone_box()
    }
}

pub struct Obj<T: ObjectType + ?Sized> {
    id: u64,
    saved: bool,
    dirty: bool,
    data: Option<Box<T>>,
    _object_type: PhantomData<T>,
}

impl<T: ObjectType> Obj<T> {
    pub fn new() -> Obj<T> {
        Obj {
            id: 0,
            saved: false,
            dirty: true,
            data: None,
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
//    fn deserialize() -> impl Fn(&[u8]) -> Result<Self, bincode::Error> {
//        |bytes| bincode::deserialize(bytes)
//    }
}

pub struct ObjectStore {
    record_store: RecordStore,
    pub object_factory: TypeRegistry,
}

impl ObjectStore {

    pub fn new(base_dir: &str) -> ObjectStore {
        let record_store = RecordStore::new( base_dir );

        ObjectStore {
            record_store,
            object_factory: TypeRegistry::new(),
        }
    }

    pub fn new_obj(&mut self, object_type_name: String) -> Result<Box<Obj<dyn ObjectType>>, RecordStoreError> {
        Ok(self.object_factory.new_obj( &object_type_name )?)
    }

    pub fn save_obj(&mut self,obj: &Obj<dyn ObjectType>) -> Result<(),RecordStoreError> {
        match &obj.data {
            Some(data) => {
                let serialized_bytes = bincode::serialize(&data).unwrap();
                let name = data.name();
                let wrapper = RegistryWrapper::new( name, &serialized_bytes );
                let serialized_bytes = bincode::serialize(&wrapper).unwrap();
                self.record_store.stow( obj.id as usize, &serialized_bytes )?;
                Ok(())
            },
            None => Err(RecordStoreError::ObjectStore(String::from("save_obj: no data found")))
        }
    }

    pub fn fetch(&mut self, id: usize) -> Result<Box<Obj<dyn ObjectType>>,RecordStoreError> {
        let serialized_bytes = self.record_store.fetch( id )?.unwrap();
        let wrapper: RegistryWrapper = bincode::deserialize(&serialized_bytes).unwrap();
        Ok(self.object_factory.deserialize_obj( &wrapper.type_name, wrapper.bytes )?)
/*
        let obj_data = self.object_factory.deserialize_obj( &wrapper.type_name, wrapper.bytes )?;
        let obj = Obj {
            id: id as u64,
            saved: true,
            dirty: false,
            data: Some(obj_data),
            _object_type: PhantomData,
        };
        Ok(Box::new(obj))
*/
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
        wingspan: u64,
    }

    impl ObjectType for Canary {
        fn name(&self) -> String { String::from("canary") }
    }


    #[test]
    fn object_store() {
        let testdir = TempDir::new().expect("coult not open testdir");
        let testdir_path = testdir.path().to_string_lossy().to_string();

        let mut os = ObjectStore::new( &testdir_path );
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
        
    }
}
