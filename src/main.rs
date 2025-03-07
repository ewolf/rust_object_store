use yote_recordstore_rust::object_store::{ObjectStore, ObjectType};
use yote_recordstore_rust::recycle_silo::SiloByteData;

use serde::{Serialize, Deserialize};
use bincode;

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
struct CoalMine {
    tons: usize,
}
impl ObjectType for CoalMine {
    fn new() -> Box<CoalMine> {
        Box::new(CoalMine { tons: 0 })
    }
    fn load(bytes: &[u8]) -> Box<CoalMine> {
        let coalmine: CoalMine = bincode::deserialize(bytes).expect("coalmine load failed");
        Box::new( coalmine )
    }
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
