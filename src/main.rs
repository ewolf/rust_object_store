use yote_recordstore_rust::object_store::*;
use recordstore_macros::*;
use yote_recordstore_rust::silo::RecordStoreError;
use std::collections::HashMap;

init_objects!();

#[derive(Serialize, Deserialize, Getters, Debug)]
struct CueStore {
    cues: Vec<String>,
    cue2idx: HashMap<String,u32>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct U32Vec {
    vec: Vec<u32>,
}
impl VecObjectTypeExt for Obj<U32Vec> {
    fn get(&self, key: usize) -> Option<&ObjectTypeOption> {
        self.data.vec.get(key)
    }
    fn push(&mut self, val: ObjectTypeOption) {
        self.dirty = true;
        self.data.vec.push(val);
    }
    fn len(&self) -> usize {
        self.data.vec.len()
    }
    fn insert(&mut self, key: usize, val: ObjectTypeOption) {
        self.dirty = true;
        self.data.vec.insert(key, val);
    }
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct U32VecVec {
    vec: Vec<Ref>,
}
impl VecObjectTypeExt for Obj<U32VecVec> {
    fn get(&self, key: usize) -> Option<&ObjectTypeOption> {
        self.data.vec.get(key)
    }
    fn push(&mut self, val: ObjectTypeOption) {
        self.dirty = true;
        self.data.vec.push(val);
    }
    fn len(&self) -> usize {
        self.data.vec.len()
    }
    fn insert(&mut self, key: usize, val: ObjectTypeOption) {
        self.dirty = true;
        self.data.vec.insert(key, val);
    }
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct Exemplars {
    cuestore_ref: Ref,
    seq_exems_ref: Ref,
    uniq_exems_ref: Ref,
}


use std::fs::File;
use std::io::{BufRead, BufReader};
use regex::Regex;
fn load_exems(file_name: &str, object_store: ObjectStore) 
              -> Result<Box<Obj<Exemplars>>,RecordStoreError> 
{
    let binding = object_store.record_store_binding();
    let mut record_store = binding.lock().unwrap();

    let mut root = object_store.fetch_root_rs(&mut record_store);

    if let Some(ObjectTypeOption::Ref(exems_ref)) = root.get("exemplars") {
        let exems: Exemplars = object_store.fetch_rs(&mut record_store, exems_ref.id)?;
        let uniq_exems = object_store.fetch_rs(&mut record_store, exems.get_uniq_exems_ref().id )?;
        eprintln!( "ALREADY HAVE THE exems {}. id {}", uniq_exems.data.len(), exems.id );

        return Ok(exems);
    }

    let reader = BufReader::new(File::open(file_name)?);

    let mut cues: Vec<String> = Vec::new();
    let mut cue2idx: HashMap<String,u32> = HashMap::new();

    let mut seq_exems_vec: Vec<Ref> = Vec::new();
    let mut uniq_exems_vec: Vec<Ref> = Vec::new();

    let mut needs_title = true;
    let mut title = String::new();

    for line_result in reader.split(b'\n') {
        let line_bytes = line_result?;
        let line = String::from_utf8_lossy(&line_bytes);
        if needs_title {
            title = line.to_string();
            println!("ARTICLE {}", title);
            needs_title = false;
        } else {
            let line = line.to_lowercase();
            let notags_rx = Regex::new(r"<[^>]*>").unwrap();
            let alpha_only_rx = Regex::new(r"[^a-z]").unwrap();
            let split_rx = Regex::new(r" +").unwrap();

            let line = notags_rx.replace_all(&line,"");
            let line = alpha_only_rx.replace_all(&line," ");

            let words: Vec<&str> = split_rx
                .split(&line)
                .collect();

            let mut seq_cue_idxs: Vec<u32> = Vec::new();

            for word in &words {
                let word_str = word.to_string();
                match cue2idx.get(&word_str) {
                    None => {
                        let idx: u32 = cues.len()  as u32;
                        cues.push(word_str.clone());
                        cue2idx.insert(word_str.clone(), idx);
                        seq_cue_idxs.push(idx);
                    },
                    Some(idx) => {
                        seq_cue_idxs.push(idx.clone());
                    }
                }
            }

            let seq_exem = object_store
                .new_obj_rs( &mut record_store, U32Vec { vec: seq_cue_idxs } )?;
            
            seq_exems_vec.push( seq_exem.make_ref() );

            let mut uniq_cue_idxs = seq_cue_idxs.clone();
            uniq_cue_idxs.sort_unstable();
            uniq_cue_idxs.dedup();

            let uniq_exem = object_store
                .new_obj_rs( &mut record_store, U32Vec { vec: uniq_cue_idxs } )?;
            
            uniq_exems_vec.push( uniq_exem.make_ref() );

            needs_title = true;
        }
    }

    let cuestore = object_store.new_obj_rs( &mut record_store, 
                                             CueStore { cues, cue2idx } )?;
    let seq_exems = object_store.new_obj_rs( &mut record_store, 
                                              U32VecVec { vec: seq_exems_vec } )?;
    let uniq_exems = object_store.new_obj_rs( &mut record_store, 
                                               U32VecVec { vec: uniq_exems_vec } )?;

    let exems = object_store.new_obj_rs( &mut record_store, 
                                          Exemplars { 
                                              cuestore_ref: cuestore.make_ref(),
                                              seq_exems_ref: seq_exems.make_ref(),
                                              uniq_exems_ref: uniq_exems.make_ref(),
                                          } )?;

    root.put( "exems", exems.make_ref_opt() );

    let _ = object_store.save_obj_rs( &mut record_store, &mut root );
    Ok(exems)
}

fn main() {

    let exem_os = ObjectStore::new("./data/exems");
    
    let exems = load_exems("./source_data/all_articles.txt", exem_os).expect("got exems");
    let seq_exems = exem_os.fetch::<U32VecVec>( exems.get_seq_exems_ref().id ).expect("No way");
    eprintln!(" exems howmany ? {}", seq_exems.data.len() );


    println!("HI");
}
