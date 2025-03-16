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
struct Exemplars {
    cue_store_ref: ObjectTypeOption::Reference<CueStore>,
    seq_exems_ref: ObjectTypeOption::Reference<Vec<Vec<u32>>>,
    uniq_exems_ref: ObjectTypeOption::Reference<Vec<Vec<u32>>>,
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

    if let Some(ObjectTypeOption::Reference(exems)) = root.get("exemplars") {
        let exems = object_store.fetch_rs(&mut record_store, articles_ref.id)?;
        let uniq_exems = exems.get_uniq_exems();
        eprintln!( "ALREADY HAVE THE exems {}. id {}", articles.len(), articles_ref.id );

        return Ok(articles);
    }

    let mut articles = object_store
        .new_obj_rs( &mut record_store, VecObjectType::new() )
        .expect("articles making problem");

    root.put("articles", ObjectTypeOption::Reference( articles.make_ref()));
    let _ = object_store.save_obj_rs( &mut record_store, &mut root );

    if let Some(ObjectTypeOption::Reference(articles_ref)) = root.get("articles") {
        eprintln!( "ROOT PUT THEN GET {}", articles_ref.id );
    }

    let reader = BufReader::new(File::open(file_name)?);

    let mut cues: Vec<String> = Vec::new();
    let mut cue2idx: HashMap<String,u32> = HashMap::new();

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

            let mut uniq_cue_idxs = seq_cue_idxs.clone();
            uniq_cue_idxs.sort_unstable();
            uniq_cue_idxs.dedup();

            let article = object_store
                .new_obj_rs( &mut record_store, Article { title: title.clone(), 
                                                          seq_cue_idxs,
                                                          uniq_cue_idxs,
                } )?;
            articles.push( ObjectTypeOption::Reference(article.make_ref()) );


            needs_title = true;
        }
    }
    let cuestore = object_store.new_obj_rs( &mut record_store, CueStore { cues, cue2idx } ).expect("cuestore making problem");
    root.put("cuestore", ObjectTypeOption::Reference( cuestore.make_ref()));


    let _ = object_store.save_obj_rs( &mut record_store, &mut articles );
    Ok(articles)
}

fn main() {

    let article_os = ObjectStore::new("./data/articles");
    let articles = load_articles("./source_data/all_articles.txt", article_os).expect("got articles");
    eprintln!(" Articles howmany ? {}", articles.len() );


    println!("HI");
}
