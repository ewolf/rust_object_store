use yote_recordstore_rust::object_store::*;
use recordstore_macros::*;
use yote_recordstore_rust::silo::RecordStoreError;
use std::collections::HashMap;

init_objects!();

struct Config {
    min_word_length: usize,
    max_word_length: usize,

    min_coinc_cue_perc: usize,
    max_coinc_cue_perc: usize,

    min_coinc_cue_freq: usize,

    min_overall_cue_perc: f32,
    max_overall_cue_perc: f32,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct CueStore {
    cues: Vec<String>,
    cue2idx: HashMap<String,u32>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct U32Vec {
    vec: Vec<u32>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct RefVec {
    vec: Vec<Ref>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct Exemplars {
    cuestore_ref: Ref,

    // glo - cues are indexed in the order they are encountered
    glo_tot_freq_ref: Ref,
    glo_exem_freq_ref: Ref,

    glo_seq_exems_ref: Ref,
    glo_uniq_exems_ref: Ref,

    glo_to_loc_ref: Ref,
    loc_to_glo_ref: Ref,

    // loc - cues are sorted by descending frequency
    loc_tot_freq_ref: Ref,
    loc_exem_freq_ref: Ref,
}


use std::fs::File;
use std::io::{BufRead, BufReader};
use regex::Regex;
fn load_exems(file_name: &str, object_store: &ObjectStore, config: &Config)
              -> Result<Box<Obj<Exemplars>>,RecordStoreError>
{
    let binding = object_store.record_store_binding();
    let mut record_store = binding.lock().unwrap();

    let mut root = object_store.fetch_root_rs(&mut record_store);

    if let Some(ObjTypeOption::Ref(exems_ref)) = root.get("exems") {
        let exems = object_store.fetch_rs(&mut record_store, exems_ref.id)?;
        let glo_uniq_exems = object_store.fetch_rs::<RefVec>(&mut record_store, exems.get_glo_uniq_exems_ref().id )?;
        eprintln!( "ALREADY HAVE THE exems {}. id {}", glo_uniq_exems.data.vec.len(), exems.id );

        return Ok(exems);
    }

    eprintln!("opening {}", file_name);
    let reader = BufReader::new(File::open(file_name)?);

    eprintln!("creating reserved vec");
    let mut cues: Vec<String> = Vec::new();
    cues.reserve(10_000_000);
    let mut cue2idx: HashMap<String,u32> = HashMap::new();

    let mut glo_tot_freq: Vec<u32> = vec![0u32; 10_000_000];

    let mut glo_exem_freq: Vec<u32> = vec![0u32; 10_000_000];

    let mut glo_seq_exems_vec: Vec<Ref> = Vec::new();
    glo_seq_exems_vec.reserve(10_000_000);
    let mut glo_uniq_exems_vec: Vec<Ref> = Vec::new();
    glo_uniq_exems_vec.reserve(10_000_000);

    let mut needs_title = true;
    eprintln!("reading articles");
    for line_result in reader.split(b'\n') {
        let line_bytes = line_result?;
        let line = String::from_utf8_lossy(&line_bytes);
        if needs_title {
            println!("ARTICLE {}", line);
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
                if word.len() >= config.min_word_length
                    && word.len() <= config.max_word_length 
                { 
                    let word_str = word.to_string();
                    match cue2idx.get(&word_str) {
                        None => {
                            let idx: u32 = cues.len()  as u32;
                            cues.push(word_str.clone());
                            cue2idx.insert(word_str.clone(), idx);
                            seq_cue_idxs.push(idx);
                            glo_tot_freq[idx as usize] = 1;
                        },
                        Some(idx) => {
                            seq_cue_idxs.push(idx.clone());
                            glo_tot_freq[*idx as usize] += 1;
                        }
                    }
                }
            }

            let mut uniq_cue_idxs = seq_cue_idxs.clone();
            uniq_cue_idxs.sort_unstable();
            uniq_cue_idxs.dedup();

            for idx in &uniq_cue_idxs {
                glo_exem_freq[*idx as usize] += 1;
            }

            let seq_exem = object_store
                .new_obj_rs( &mut record_store, U32Vec { vec: seq_cue_idxs } )?;

            glo_seq_exems_vec.push( seq_exem.make_ref() );


            let glo_uniq_exem = object_store
                .new_obj_rs( &mut record_store, U32Vec { vec: uniq_cue_idxs } )?;

            glo_uniq_exems_vec.push( glo_uniq_exem.make_ref() );

            needs_title = true;
        }
    }

    // fit the vectors
    cues.shrink_to_fit();
    glo_tot_freq.resize(cues.len(), 0);
    glo_exem_freq.resize(cues.len(), 0);
    glo_seq_exems_vec.shrink_to_fit();
    glo_uniq_exems_vec.shrink_to_fit();

    // sort the glo into loc
    let mut loc_to_glo: Vec<u32> = (0..glo_tot_freq.len()).map(|i| i as u32 ).collect();
    loc_to_glo.sort_by_key( |&i| std::cmp::Reverse(&glo_tot_freq[i as usize]) );
    let mut glo_to_loc: Vec<u32> = Vec::new();
    glo_to_loc.reserve(glo_tot_freq.len());
    for loc_idx in &loc_to_glo {
        glo_to_loc.insert( loc_to_glo[*loc_idx as usize] as usize, *loc_idx as u32);
    }
    let mut loc_exem_freq: Vec<u32> = Vec::new();
    loc_exem_freq.reserve(glo_tot_freq.len());
    let mut loc_tot_freq: Vec<u32> = Vec::new();
    loc_tot_freq.reserve(glo_tot_freq.len());
    for glo_idx in (0..loc_to_glo.len()).collect::<Vec<_>>() {
        loc_exem_freq.insert( glo_to_loc[glo_idx] as usize, glo_exem_freq[glo_idx] );
        loc_tot_freq.insert( glo_to_loc[glo_idx] as usize, glo_tot_freq[glo_idx] );
    }

    let loc_to_glo = object_store.new_obj_rs( &mut record_store,
                                               U32Vec { vec: loc_to_glo } )?;
    let glo_to_loc = object_store.new_obj_rs( &mut record_store,
                                               U32Vec { vec: glo_to_loc } )?;
    let cuestore = object_store.new_obj_rs( &mut record_store,
                                             CueStore { cues, cue2idx } )?;
    let glo_seq_exems = object_store.new_obj_rs( &mut record_store,
                                                     RefVec { vec: glo_seq_exems_vec } )?;
    let glo_uniq_exems = object_store.new_obj_rs( &mut record_store,
                                                      RefVec { vec: glo_uniq_exems_vec } )?;
    let glo_tot_freq = object_store.new_obj_rs( &mut record_store,
                                                     U32Vec { vec: glo_tot_freq } )?;
    let glo_exem_freq = object_store.new_obj_rs( &mut record_store,
                                                          U32Vec { vec: glo_exem_freq } )?;
    let loc_tot_freq = object_store.new_obj_rs( &mut record_store,
                                                      U32Vec { vec: loc_tot_freq } )?;
    let loc_exem_freq = object_store.new_obj_rs( &mut record_store,
                                                      U32Vec { vec: loc_exem_freq } )?;
    let exems = object_store.new_obj_rs( &mut record_store,
                                          Exemplars {
                                              cuestore_ref: cuestore.make_ref(),
                                              glo_seq_exems_ref: glo_seq_exems.make_ref(),
                                              glo_uniq_exems_ref: glo_uniq_exems.make_ref(),
                                              glo_tot_freq_ref: glo_tot_freq.make_ref(),
                                              glo_exem_freq_ref: glo_exem_freq.make_ref(),
                                              glo_to_loc_ref: glo_to_loc.make_ref(),
                                              loc_to_glo_ref: loc_to_glo.make_ref(),
                                              loc_tot_freq_ref: loc_tot_freq.make_ref(),
                                              loc_exem_freq_ref: loc_exem_freq.make_ref(),
                                          } )?;

    root.put( "exems", exems.make_ref_opt() );

    let _ = object_store.save_obj_rs( &mut record_store, &mut root );
    Ok(exems)
}

fn chunk_cues_by_freq( freqs: &Vec<u32>, min_cue_idx: usize, max_cue_idx: usize ) -> Vec<usize> {
    let max_chunk_size = 5000;
    let thresh_fract = 700;

    let mut tot = 0;
    for idx in (min_cue_idx..max_cue_idx).collect::<Vec<_>>() {
        tot += freqs[idx];
    }

    let thresh = tot / thresh_fract;
    let mut chunk_number_freqs = 0;

    let mut cue_chunks: Vec<usize> = Vec::new();
    let mut f = 0;
    for idx in (min_cue_idx..max_cue_idx).collect::<Vec<_>>() {
        f += freqs[idx];
        chunk_number_freqs += 1;
        if f > thresh || chunk_number_freqs > max_chunk_size {
            cue_chunks.push( chunk_number_freqs );
            chunk_number_freqs = 0;
            f = 0;
        }
    }

    cue_chunks
}

fn find_coincs(exem_os: &ObjectStore, coinc_os: &ObjectStore, config: &Config) 
               -> Result<Box<Obj<U32Vec>>,RecordStoreError>
{
    let binding = exem_os.record_store_binding();
    let mut exem_rs = binding.lock().unwrap();
    let mut exem_root = exem_os.fetch_root_rs(&mut exem_rs);

    let binding = coinc_os.record_store_binding();
    let mut coinc_rs = binding.lock().unwrap();
    let mut coinc_root = coinc_os.fetch_root_rs(&mut coinc_rs);

    if let Some(ObjTypeOption::Ref(coincs_ref)) = coinc_root.get("coincs") {
        let coincs = coinc_os.fetch_rs::<U32Vec>(&mut coinc_rs, coincs_ref.id )?;
        eprintln!("Found {} coincs", coincs.data.vec.len() );
        return Ok(coincs);
    }


    if let Some(ObjTypeOption::Ref(exems_ref)) = exem_root.get("exems") {
        let exems = exem_os.fetch_rs(&mut exem_rs, exems_ref.id)?;
        let loc_tot_freq = exem_os.fetch_rs::<U32Vec>(&mut exem_rs, exems.get_loc_tot_freq_ref().id )?;

//        let chunks = chunk_cues_by_freq( loc_tot_freq, config );
    } 
      
    Err(RecordStoreError::ObjectStore("HBEEEL".to_string()))
}

fn main() {

    let conf = Config {
        min_word_length: 2,
        max_word_length: 12,

        min_coinc_cue_perc: 1,
        max_coinc_cue_perc: 30,

        min_coinc_cue_freq: 100,

        min_overall_cue_perc: 0.5,
        max_overall_cue_perc: 40.0,

    };


// no word limits: 943M	data, 

    let exem_os = ObjectStore::new("./data/exems");
    let coinc_os = ObjectStore::new("./data/coinc");

    let exems = load_exems("./source_data/all_articles.txt", &exem_os, &conf).expect("got exems");

    let seq_exems = exem_os.fetch::<RefVec>( exems.get_glo_seq_exems_ref().id ).expect("No way");
    eprintln!(" exems howmany ? {}", seq_exems.data.vec.len() );

    find_coincs( &exem_os, &coinc_os, &conf ).expect("could not coincs");
    
    println!("HI");
}
