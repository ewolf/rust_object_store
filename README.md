This is an experiment to create a path based rather than table based object store. 
This one stores its contents in an array of fixed record binary files.

WHAT IT DOES

Objects are contained within a rooted graph that allows for circular and multiple references.
ChatGPT says the formal name for this is a 'rooted directed graph' or 'rooted cyclic graph'. 
The ObjectStore manages this graph, and saves and loads objects. Each object is assigned an
id by the store. The store keeps a Vec of objects that are in a dirty state so that a save
saves all dirty objects. An object can be loaded by a forward slash path delimeter that 
traces back to the root, or by id. An object can provide other objects attached to it using
lazy lookup. An object does not contain an actual pointer to other objects,
just their ids in the store.

The GOAL:
  use yote_recordstore_rust::object_store::ObjectStore;
  let object_store = ObjectStore::new("my-directory".to_string());
  let my_baz: MyStruct = object_store.fetch( "/foo/bar/baz" );
  // where foo is an object and a field of the root
  //       bar is an object and a field off of foo
  //       baz is an object and a field off of bar

WHY IT DOES WHAT IT DOES:

its easier to use a path to lookup.

CURRENT STATUS:
   
   
