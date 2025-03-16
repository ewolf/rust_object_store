use yote_recordstore_rust::object_store::*;
use recordstore_macros::*;
use yote_recordstore_rust::silo::RecordStoreError;

use regex::Regex;

use statrs::statistics::Statistics;

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    sync::{mpsc,Arc},
    thread,
};

init_objects!();

struct Config {
    max_exems: u32,

    exem_chunk_size: u32,
    window_range: u32,

    min_word_length: u32,
    max_word_length: u32,

    min_coinc_cue_perc: f32,
    max_coinc_cue_perc: f32,

    min_coinc_cue_freq: u32,

    min_overall_cue_perc: f32,
    max_overall_cue_perc: f32,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
pub struct RefVec {
    vec: Vec<Ref>,
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
struct U32HashMap {
    hash: HashMap<u32,u32>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct U32F32HashMap {
    hash: HashMap<u32,f32>,
}

#[derive(Serialize, Deserialize, Getters, Debug)]
struct Exems {
    cuestore_ref: Ref,  // to CueStore

    exem_count: usize,

    // glo - cues are indexed in the order they are encountered
    glo_tot_freq_ref: Ref,  // to U32Vec
    glo_exem_freq_ref: Ref, // to U32Vec

    glo_seq_exems_ref: Ref,  // to RefVec of U32Vec
    glo_uniq_exems_ref: Ref, // to RefVec of U32Vec

    glo_to_loc_ref: Ref, // to U32Vec
    loc_to_glo_ref: Ref, // to U32Vec

    // loc - cues are sorted by descending frequency
    loc_tot_freq_ref: Ref,  // to U32Vec
    loc_exem_freq_ref: Ref, // to U32Vec
}


fn load_exems(file_name: &str, object_store: &ObjectStore, config: &Config)
              -> Result<Box<Obj<Exems>>,RecordStoreError>
{
    let binding = object_store.record_store_binding();
    let mut record_store = binding.lock().unwrap();

    let mut root = object_store.fetch_root_rs(&mut record_store);

    if let Some(ObjTypeOption::Ref(exems_ref)) = root.get("exems") {
        let exems = object_store.fetch_rs(&mut record_store, exems_ref.id)?;

        let glo_uniq_exems = object_store.fetch_rs::<RefVec>(&mut record_store,
                                                             exems.get_glo_uniq_exems_ref().id )?;
        eprintln!( "LOADED exems FROM store {}. id {}", glo_uniq_exems.get_vec().len(), exems.id );

        return Ok(exems);
    }

    eprintln!("reading exems from file {}", file_name);
    let reader = BufReader::new(File::open(file_name)?);

    let mut cues: Vec<String> = Vec::with_capacity(10_000_000);
    let mut cue2idx: HashMap<String,u32> = HashMap::new();

    let mut glo_tot_freq: Vec<u32> = vec![0u32; 10_000_000];

    let mut glo_exem_freq: Vec<u32> = vec![0u32; 10_000_000];

    let mut glo_seq_exems_vec: Vec<Ref> = Vec::with_capacity(10_000_000);
    let mut glo_uniq_exems_vec: Vec<Ref> = Vec::with_capacity(10_000_000);

    let mut needs_title = true;

    for line_result in reader.split(b'\n') {
        let line_bytes = line_result?;
        let line = String::from_utf8_lossy(&line_bytes);
        if needs_title {
            println!("READING ARTICLE {}", line);
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
                let wordlen: u32 = word.len() as u32;
                if wordlen >= config.min_word_length
                    && wordlen <= config.max_word_length
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

            if config.max_exems > 0 && glo_uniq_exems_vec.len() > config.max_exems as usize {
                break;
            }
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
    let mut glo_to_loc: Vec<u32> = vec![0; glo_tot_freq.len()];
    for loc_idx in &loc_to_glo {
        glo_to_loc.insert( loc_to_glo[*loc_idx as usize] as usize, *loc_idx as u32);
    }
    let mut loc_exem_freq: Vec<u32> = vec![0; glo_tot_freq.len()];
    let mut loc_tot_freq: Vec<u32> = vec![0; glo_tot_freq.len()];
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
                                          Exems {
                                              exem_count: glo_seq_exems.get_vec().len(),
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

fn chunk_cues_by_freq( freqs: &Vec<u32>, start_cue_idx: u32, end_cue_idx: u32 ) -> Vec<u32> {
    let max_chunk_size = 5000;
    let thresh_fract = 700;

    let mut tot: u32 = 0;
    for idx in (start_cue_idx..end_cue_idx).collect::<Vec<u32>>() {
        tot += freqs[idx as usize];
    }

    let thresh = tot / thresh_fract;
    let mut chunk_number_freqs = 0;

    let mut cue_chunks = Vec::new();
    let mut f = 0;
    for idx in (start_cue_idx..end_cue_idx).collect::<Vec<_>>() {
        f += freqs[idx as usize];
        chunk_number_freqs += 1;
        if f > thresh || chunk_number_freqs > max_chunk_size {
            cue_chunks.push( chunk_number_freqs );
            chunk_number_freqs = 0;
            f = 0;
        }
    }

    cue_chunks
}

fn find_cue_endpoints(freq: &Obj<U32Vec>,
                      exems: &Obj<Exems>,
                      config: &Config)
                      -> (u32,u32,u32,u32)
{

    let freq_vec = freq.get_vec();

    let mut starting_cue_idx: u32 = 0;
    let mut ending_cue_idx = freq_vec.len() as u32;
    let mut min_overall_cue_idx: u32 = 0;
    let mut max_overall_cue_idx = ending_cue_idx;

    if config.min_coinc_cue_perc > 0.0
        || config.max_coinc_cue_perc > 0.0
        || config.min_overall_cue_perc > 0.0
        || config.max_overall_cue_perc  > 0.0
    {
        let exem_count: f32 = *(exems.get_exem_count()) as f32;
        for idx in 0..freq_vec.len() {
            if let Some(freq_int) = freq_vec.get(idx) {
                let freq: f32 = *freq_int as f32;
                let freq_usize: u32 = *freq_int as u32;
                let perc: f32 = 100.0 * freq / exem_count;
                if perc <= config.min_coinc_cue_perc && starting_cue_idx > 0 && ending_cue_idx == 0 {
                    starting_cue_idx = idx as u32;
                }
                else if perc <= config.min_coinc_cue_perc && starting_cue_idx > 0 && ending_cue_idx == 0 {
                    ending_cue_idx = idx as u32;
                }
                else if freq_usize <= config.min_coinc_cue_freq && starting_cue_idx > 0 && ending_cue_idx == 0 {
                    ending_cue_idx = idx as u32;
                }

                if perc >= config.min_overall_cue_perc && perc <= config.max_overall_cue_perc && min_overall_cue_idx == 0 {
                    min_overall_cue_idx = idx as u32;
                }
                else if perc <= config.min_overall_cue_perc && min_overall_cue_idx > 0 && max_overall_cue_idx == 0 {
                    max_overall_cue_idx = idx as u32;
                }
            }
        }
    }

    return (starting_cue_idx,
            ending_cue_idx,
            min_overall_cue_idx,
            max_overall_cue_idx);
}

fn find_coincs(exem_os: &ObjectStore,
               coinc_os: &ObjectStore,
               config: &Config)
               -> Result<Box<Obj<RefVec>>,RecordStoreError>
{
    let exem_root = exem_os.fetch_root();

    let mut coinc_root = coinc_os.fetch_root();

    eprintln!("FETCH ROOT ID {}", coinc_root.id );

    // coincs are stored in a data structure like so:
    //   [ { cue_idx => count, ... }, { cue_idx => count, .. }, ... ]
    //  where each idx in the vec is a cue idx

    if let Some(ObjTypeOption::Bool(_)) = coinc_root.get("has_coincs") {
        let ObjTypeOption::Ref(coincs_ref) = coinc_root.get("coincs").unwrap() else {
            return Err(RecordStoreError::ObjectStore("has_coincs, but no coincs found".to_string()));
        };
        let coincs = coinc_os.fetch::<RefVec>( coincs_ref.id )?;
        eprintln!("Found {} coincs at id {}", coincs.get_vec().len(), coincs_ref.id );
        return Ok(coincs);
    }

    eprintln!("Calculating coincs");

    let ObjTypeOption::Ref(exems_ref) = exem_root.get("exems").unwrap() else {
            return Err(RecordStoreError::ObjectStore("exems are missing".to_string()));
    };
    let exems = exem_os.fetch::<Exems>(exems_ref.id)?;

    let exem_count: u32 = *exems.get_exem_count() as u32;
    let loc_tot_freq = exem_os.fetch::<U32Vec>( exems.get_loc_tot_freq_ref().id )?;

    // start_cue_idx, end_cue_idx are the ranges that coincs will be calcualted for
    //   cues within the min_overall_cue_idx to max_overall_cue_idx range will be included others' coincs
    //   but will not have coincs themselves
    let (start_cue_idx, end_cue_idx, min_overall_cue_idx, max_overall_cue_idx)
        = find_cue_endpoints( &loc_tot_freq, &exems, config );

    // fill this up with empty u32hashmap objects and save it, then update
    //    u32hashmap objects when calculating
    let coincs = {
        let mut coin = coinc_os.new_obj( RefVec { vec: Vec::new() } )?;
        for cue_idx in start_cue_idx..end_cue_idx {
            let mut map = coinc_os.new_obj( U32HashMap { hash: HashMap::new() } )?;
            coin.data.vec.insert( cue_idx as usize, map.make_ref() );
            let _ = coinc_os.save_obj( &mut map );
        }
        coinc_root.put("coincs", coin.make_ref_opt() );
        let _ = coinc_os.save_obj( &mut coinc_root );
        let _ = coinc_os.save_obj( &mut coin );
        coin
    };

    let cue_chunks = chunk_cues_by_freq( loc_tot_freq.get_vec(), start_cue_idx, end_cue_idx );

    let exem_chunk_size = if config.exem_chunk_size < exem_count { config.exem_chunk_size } else { exem_count };
    let window_range = config.window_range;

    let mut start_cue_idx = start_cue_idx;
    let mut exem_idx: u32 = 0;

    let rv = exem_os.fetch::<RefVec>( exems.get_glo_seq_exems_ref().id )?;
    let exem_glo_seqs = rv.get_vec();
    let rv = exem_os.fetch::<U32Vec>( exems.get_glo_to_loc_ref().id )?;
    let glo_to_loc = rv.get_vec();

    while exem_idx < exem_count {
        let seq_chunk = Arc::new({
            let mut chunk: Vec<Vec<u32>> = Vec::with_capacity(exem_chunk_size as usize);
            for chunk_idx in 0..(exem_chunk_size-1) {
                let v = exem_os.fetch::<U32Vec>( exem_glo_seqs.get( chunk_idx as usize ).expect("FOO").id)?;
                let exem_glo_seq = v.get_vec();
                chunk.push( exem_glo_seq.iter().map( |&glo_idx| { glo_to_loc[glo_idx as usize] } ).collect::<Vec<_>>());
            }
            chunk
        });

        exem_idx += exem_chunk_size;

        let (tx, rx) = mpsc::channel::<(_,_)>();

        let mut handles = Vec::new();

        for cue_bunch in &cue_chunks {
            let cue_idx_start_range = start_cue_idx;
            let cue_idx_end_range = cue_idx_start_range + (cue_bunch - 1);
            let chunk_size = (cue_idx_end_range - cue_idx_start_range) + 1;
            start_cue_idx = cue_idx_end_range + 1;

            //
            // in thread
            //
            let tx_clone = tx.clone();

            let seq_chunk = Arc::clone( &seq_chunk );

            let handle = thread::spawn( move || {
                //
                // prep data structures
                //

                //
                // a vec for the active cues where index 0 is cue_idx_start_range
                //                                       1 is cue_idx_start_range + 1
                //                                    etc until the last is cue_idx_end_range
                //
                let mut active_cueidx2coincs: Vec<HashMap<u32,u32>>
                    = Vec::with_capacity( chunk_size as usize );

                // fill with empty hashmaps
                for _ in 0..chunk_size {
                    //
                    let hm: HashMap<u32,u32> = HashMap::with_capacity( 10_000 );
                    active_cueidx2coincs.push( hm );

                }

                //
                // find the coincs windowed exem
                //
                // exem_seq is Vec<u32>
                for exem_seq in seq_chunk.iter() {
                    let exem_seq_size: u32 = exem_seq.len() as u32;
                    let mut window_endpoints: Vec<u32> = Vec::new();
                    let mut has_start = false;
                    let mut start: u32 = 0;
                    let mut end: u32 = 0;

                    for idx_in_seq in 0..exem_seq.len() {
                        let cue_idx = exem_seq[idx_in_seq];

                        // found an end
                        if has_start &&
                            idx_in_seq as u32 == end &&
                            ! (cue_idx >= cue_idx_start_range && cue_idx <= cue_idx_end_range)
                        {
                            window_endpoints.push( start );
                            window_endpoints.push( end );
                            has_start = false;
                        }

                        // cue is in range of cues we are actively building windows from
                        if cue_idx >= cue_idx_start_range && cue_idx <= cue_idx_end_range {
                            if has_start {
                                if (end + window_range) >= exem_seq_size {
                                    end = exem_seq_size;
                                } else {
                                    end +=window_range;
                                }
                            } else {
                                start = idx_in_seq as u32;
                                has_start = true;
                                end = start + 2 + window_range;
                                if end >= exem_seq_size {
                                    end = exem_seq_size;
                                }
                            }
                        }
                    }
                    window_endpoints.reverse();

                    while window_endpoints.len() > 0 {
                        let start: usize = window_endpoints.pop().expect("FO") as usize;
                        let end: usize = window_endpoints.pop().expect("FO") as usize;

                        // sorted ascending
                        let mut uniq_window_cues: Vec<u32> = exem_seq[start..end]
                            .iter()
                            .map(|v| *v)
                            .filter(|v| v >= &min_overall_cue_idx && v <= &max_overall_cue_idx)
                            .collect::<Vec<_>>();
                        uniq_window_cues.sort_unstable();
                        uniq_window_cues.dedup();

                        let active_window_cues: Vec<u32> = uniq_window_cues.clone()
                            .iter()
                            .map(|v| *v)
                            .filter(|v| v >= &cue_idx_start_range && v <= &cue_idx_end_range )
                            .collect::<Vec<_>>();

                        for active_cue_idx in &active_window_cues {
                            for seq_cue_idx in &uniq_window_cues {
                                if seq_cue_idx != active_cue_idx {
                                    *active_cueidx2coincs[(*active_cue_idx - cue_idx_start_range) as usize].entry(*seq_cue_idx).or_insert(0) += 1;
                                }
                            }
                        }
                    } // for each window
                } // find coincs in windowed exem

                let result = (cue_idx_start_range, active_cueidx2coincs);
                eprintln!("SEND for {}",cue_idx_start_range);
                let _ = tx_clone.send(result).unwrap();
            });
            handles.push(handle);

        } //each cue_bunch in cue_chunks

        drop(tx);

        // can now collect the results
        for handle in handles {
            handle.join().unwrap();
        }

        for received in rx {
            let (cue_idx_start_range, active_cueidx2coincs): (_, Vec<HashMap<u32,u32>>) = received;

            for offset in 0..active_cueidx2coincs.len() {
                let a_cue_idx = cue_idx_start_range as usize  + offset;

                if let Some(delta_map) = active_cueidx2coincs.get(a_cue_idx) {
                    let mut idx_coincs = coinc_os.fetch::<U32HashMap>(
                        coincs.get_vec().get( a_cue_idx ).unwrap().id
                    ).unwrap();
                    for (b_cue_idx, count) in delta_map {
                        *idx_coincs.data.hash.entry(*b_cue_idx).or_insert(0) += count;
                    }
                    let _ = coinc_os.save_obj( &mut idx_coincs );
                }
            }

            eprintln!("received '{cue_idx_start_range}'");
        }

        eprintln!("done receiving");


        eprintln!("receiver joined");

    } // while there are exems to do

    coinc_root.put("has_coincs", ObjTypeOption::Bool(true));
    let _ = coinc_os.save_obj( &mut coinc_root );

    Ok(coincs)
}

fn calculate_rate_scores(rates: &HashMap<u32,f64>,
                         freqs: &Vec<u32>,
                         sigma_bound: f32) 
                         -> HashMap<u32,f32> {
    let mut scores = HashMap::new();
    
    let stddev = rates.values().copied().collect::<Vec<f64>>().std_dev().round() as f32;
    let avg = rates.values().copied().collect::<Vec<f64>>().mean().round() as f32;

    if stddev > 0.0 {
        for b in rates.keys() {
            let brate = (*rates.get(b).unwrap()).round() as f32;
            let sigma = ( brate - avg ) / stddev;
            if sigma >= sigma_bound {
                scores.insert( *b, (sigma.round() as f32) * (freqs[*b as usize] as f32) );
            }
        }
    }
    scores
}

fn calculate_cue_affinities(exem_os: &ObjectStore,
                            coinc_os: &ObjectStore,
                            affin_os: &ObjectStore
                           // ,config: &Config
                           )
                            -> Result<Box<Obj<RefVec>>,RecordStoreError>
{
    let mut affin_root = affin_os.fetch_root();

    if let Some(ObjTypeOption::Bool(_)) = affin_root.get("has_affinities") {
        match affin_root.get("affins").unwrap() {
            ObjTypeOption::Ref(affin_ref) => {
                let affins = affin_os.fetch::<RefVec>( affin_ref.id )?;
                eprintln!("Found {} affins at id {}", affins.get_vec().len(), affin_ref.id );
                return Ok(affins);
            },
            _ => {
                return Err(RecordStoreError::ObjectStore("has_affins, but no affins found".to_string()));
            }
         }
    }



    let coincs = {
        let coinc_root = coinc_os.fetch_root();
        if let Some(ObjTypeOption::Ref(coincs_ref)) = coinc_root.get("coincs") {
            // RefVec -> U32HashMap 
            let coin = coinc_os.fetch::<RefVec>( coincs_ref.id )?;
            coin
        } else {
            panic!( "no coincs found" );
        }
    };
    let end_cue_idx = coincs.get_vec().len();

    let exems = {
        let exem_root = exem_os.fetch_root();
        if let Some(ObjTypeOption::Ref(exems_ref)) = exem_root.get("exems") {
            let exem = exem_os.fetch::<Exems>( exems_ref.id )?;
            exem
        } else {
            panic!( "no coincs found" );
        }
    };

    let freqs = exem_os.fetch::<U32Vec>( exems.get_glo_exem_freq_ref().id )?;
    let avg = freqs.get_vec().into_iter().map(|x| *x as f64).collect::<Vec<_>>().mean();
    let min_freq = avg as f64 / 2.0;


    // fill this up with empty u32hashmap objects and save it, then update
    //    u32hashmap objects when calculating
    let affinities = {
        let mut aff = affin_os.new_obj( RefVec { vec: Vec::new() } )?;
        for cue_idx in 0..end_cue_idx {
            let mut map = affin_os.new_obj( U32F32HashMap { hash: HashMap::new() } )?;
            aff.data.vec.insert( cue_idx as usize, map.make_ref() );
            let _ = affin_os.save_obj( &mut map );
        }
        affin_root.put("affinities", aff.make_ref_opt() );
        let _ = affin_os.save_obj( &mut affin_root );
        let _ = affin_os.save_obj( &mut aff );
        aff
    };

    // do the affinity calculations here
    for local_a_idx in 0 .. end_cue_idx {
        let a_coincs_ref = coincs.get_vec().get( local_a_idx ).unwrap();
        let a_coincs = *coinc_os.fetch::<U32HashMap>(a_coincs_ref.id)?;
        let a_coincs_hash = a_coincs.get_hash();
        if a_coincs_hash.len() > 0 {
            
            let rates = {
                let mut r: HashMap<u32,f64> = HashMap::new();
                for local_b_idx in a_coincs_hash.keys() {
                    if let Some(count) = a_coincs_hash.get(local_b_idx) {
                        if *count > 0 {
                            r.insert( *local_b_idx, if *count as f64 > min_freq { *count as f64 } else { min_freq } );
                        }
                    }
                }
                r
            };

            let scores = calculate_rate_scores( &rates, &freqs.get_vec(), 3.0 );

            if scores.len() > 0 {
                // transfer the data from scores to the affinites u32f32hashmap
                let target = affin_os.fetch::<U32F32HashMap>(affinities.get_vec().get( local_a_idx ).unwrap().id)?;
                let mut target_hash = target.data.hash;
                for key in scores.keys() {
                    target_hash.insert( *key, *scores.get(key).unwrap() );
                }
            }
        }
    }

    affin_root.put("affinities", ObjTypeOption::Bool(true));
    affin_root.put("has_affinities", ObjTypeOption::Bool(true));
    let _ = affin_os.save_obj( &mut affin_root );

    Ok(affinities)
}

fn main() {

    let conf = Config {
        max_exems: 1_000,
        exem_chunk_size: 100_000,
        window_range: 7,


        min_word_length: 2,
        max_word_length: 12,

        min_coinc_cue_perc: 1.0,
        max_coinc_cue_perc: 30.0,

        min_coinc_cue_freq: 100,

        min_overall_cue_perc: 0.5,
        max_overall_cue_perc: 40.0,

    };


// no word limits: 943M	data, 910M wtih limits

    let exem_os  = ObjectStore::new("./data/exems");
    let coinc_os = ObjectStore::new("./data/coinc");
    let affin_os = ObjectStore::new("./data/affin");

    let exems = load_exems("./source_data/all_articles.txt", &exem_os, &conf).expect("got exems");
    let seq_exems = exem_os.fetch::<RefVec>( exems.get_glo_seq_exems_ref().id ).expect("No way");

    eprintln!(" exems howmany ? {}", seq_exems.get_vec().len() );
if false {

    find_coincs( &exem_os, &coinc_os, &conf ).expect("could not coincs");

//    calculate_cue_affinities( &coinc_os, &affin_os ).expect("could not affin");

    println!("HI");
}
}
