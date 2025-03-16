pub mod silo;
pub mod recycle_silo;
pub mod record_store;
pub mod object_store;

use crate::object_store::*;

use recordstore_macros::*;

init_objects!();

#[derive(Serialize, Deserialize, Getters, Debug)]
pub struct RefVec {
    pub vec: Vec<Ref>,
}
