use yote_recordstore_rust::object_store::{ObjectStore, ObjectType};
use yote_recordstore_rust::recycle_silo::SiloByteData;

use serde::{Serialize, Deserialize};
use bincode;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Canary {
    wingspan: u64,
}

impl ObjectType for Canary {
    fn name(&self) -> String { String::from("canary") }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct CoalMine {
    tons: usize,
}
impl ObjectType for CoalMine {
    fn name(&self) -> String { String::from("CoalMine") }
}


fn main() {
/*
    let mut os = ObjectStore::new( "/tmp/foofoo" );
    let _ = os.object_factory.register( "canary", 
                                         || Box::new(Canary {wingspan:0}),
                                         |bytes| { let canary: Canary = bincode::deserialize(bytes).expect("GOOGA"); Box::new(canary) } );

    os.new_obj("canary".to_string());
*/
    println!("HELLO WORLD");
}
