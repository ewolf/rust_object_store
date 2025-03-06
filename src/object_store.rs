use crate::record_store::RecordStore;
use crate::silo::RecordStoreError;

use serde::{Serialize, Deserialize};
//use std::marker::PhantomData;
use std::collections::HashMap;

trait ObjectTyper: ObjectTyperClone {
    fn name(&self) -> String;
}

trait ObjectTyperClone {
    fn clone_box(&self) -> Box<dyn ObjectTyper>;
}

impl<T: 'static + ObjectTyper + Clone>ObjectTyperClone for T {
    fn clone_box(&self) -> Box<dyn ObjectTyper> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ObjectTyper> {
    fn clone(&self) -> Box<dyn ObjectTyper> {
        self.clone_box()
    }
}

struct ObjectType {
    type_name: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Canary {
    wingspan: usize,
}

impl ObjectTyper for Canary {
    fn name(&self) -> String { String::from("Canary") }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct CoalMine {
    tons: usize,
}
impl ObjectTyper for CoalMine {
    fn name(&self) -> String { String::from("CoalMine") }
}


pub struct ObjectStore {
    record_store: RecordStore,
    object_type_registry: HashMap<String, Box<dyn ObjectTyper>>,
}

impl ObjectStore {

    pub fn new(base_dir: &str) -> ObjectStore {
        let record_store = RecordStore::new( base_dir );

        ObjectStore {
            record_store,
            object_type_registry: HashMap::new(),
        }
    }

    pub fn register_object_type(&mut self, object_type: Box<dyn ObjectTyper>) {
        self.object_type_registry.insert( object_type.name(), object_type );
    }

    pub fn new_object(&mut self, object_type_name: String) -> Result<Box<dyn ObjectTyper>, RecordStoreError> {
        match self.object_type_registry.get(&object_type_name) {
            Some(object_type) => Ok(object_type.clone()),
            None => Err( RecordStoreError::ObjectStore(String::from("Error. object type {object_type_name} is not registered"))),
        }
    }

/*
    pub fn fetch(&mut self, id: usize) {

    }
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
