use std::thread::{spawn, JoinHandle};
use std::sync::{mpsc::{SyncSender, sync_channel}, Arc, Mutex};
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;

use super::thread_pool::ThreadPool;
use super::json_line::from_json;
use super::js::ContextBuilder;
use super::result_table::ResultTable;

pub enum Reduction {
    KeyInit(Arc<String>, usize),
    FilePartInit(Arc<String>),
    FileLineInit(Arc<String>, usize, usize),
    FileLine(Arc<String>, usize, ReduceValue)
}

pub enum ReduceValue {
    FromIndex(Vec<String>),
    FromFile(String)
}

pub fn spawn_reducer(
    pool: ThreadPool,
    context_builder: Arc<ContextBuilder>,
    workers: usize,
    root_dir: &PathBuf
) -> Result<(JoinHandle<()>, SyncSender<Reduction>, ResultTable)> {
    let (reduction_sender, reduction_receiver) = sync_channel(workers);
    let result_table = ResultTable::new(root_dir)?;
    let thread_result_table = result_table.clone();
    let reducer = spawn(move|| {
        let tracker = Tracker::new();
        for reduction in reduction_receiver.iter() {
            match reduction {
                Reduction::KeyInit(key, total_parts) => {
                    tracker.new_key(key, total_parts);
                },
                Reduction::FilePartInit(key) => {
                    tracker.new_part(key);
                },
                Reduction::FileLineInit(key, current_part, total_lines) => {
                    tracker.new_line(key, current_part, total_lines);
                },
                Reduction::FileLine(key, part, result) => {
                    let context_builder = context_builder.clone();
                    let tracker = tracker.clone();
                    let results_table = thread_result_table.clone();
                    pool.execute(move|| {
                        context_builder.reuse(|context| {
                            let reduced = match result {
                                ReduceValue::FromFile(result) => {
                                    let values = from_json(&result).unwrap();
                                    context.run_reduce(&key, &values, false).unwrap()
                                },
                                ReduceValue::FromIndex(result) => {
                                    context.run_reduce(&key, &result, false).unwrap()
                                }
                            };
                            let is_part_done = tracker.save_line_result(key.clone(), part, reduced);
                            if !is_part_done {
                                return;
                            }
                            let part_values = tracker.merge_line_results(key.clone());
                            let reduced = context.run_reduce(&key, &part_values, true).unwrap();
                            let are_all_parts_done = tracker.save_part_result(key.clone(), reduced);
                            if !are_all_parts_done {
                                return;
                            }
                            let key_values = tracker.get_and_clean_key_results(key.clone());
                            let reduced = context.run_reduce(&key, &key_values, true).unwrap();
                            results_table.add(&key, &reduced).unwrap();
                        });
                    });
                }
            }
        }
    });
    
    Ok((reducer, reduction_sender, result_table))
}

struct Tracker {
    keys: Arc<Mutex<HashMap<Arc<String>, (usize, Vec<String>)>>>,
    parts: Arc<Mutex<HashMap<Arc<String>, HashMap<usize, (usize, Vec<String>)>>>>,
}

impl Clone for Tracker {
    fn clone(&self) -> Tracker {
        Tracker {
            keys: self.keys.clone(),
            parts: self.parts.clone()
        }
    }
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker {
            keys: Arc::new(Mutex::new(HashMap::new())),
            parts: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    ///Initializes a new key in the tracker with its total parts
    pub fn new_key(&self, key: Arc<String>, total_parts: usize) {
        self.keys.lock().unwrap().insert(key, (total_parts, vec![]));
    }

    ///Initializes a new part in the tracker
    pub fn new_part(&self, key: Arc<String>) {
        self.parts.lock().unwrap().insert(key, HashMap::new());
    }

    ///Initializes a new line in the tracker with its total lines for a specific part
    pub fn new_line(&self, key: Arc<String>, current_part: usize, total_lines: usize) {
        self.parts.lock().unwrap().get_mut(&key).unwrap().insert(current_part, (total_lines, vec![]));
    }

    ///Saves the line result for a line, returns true if the whole part is reduced
    pub fn save_line_result(&self, key: Arc<String>, part: usize, result: String) -> bool {
        //get a lock to the part
        let mut part_lock = self.parts.lock().unwrap();
        //get the entry to the key
        let entry = part_lock.get_mut(&key).unwrap();
        //get the sub-entry to the path
        let entry = entry.get_mut(&part).unwrap();
        //save our result
        entry.1.push(result);
        //decrement the counter of lines we need to reduce
        if entry.0 > 0 {
            entry.0 -= 1;
        }
        //is the part done?
        if entry.0 == 0 {
            true
        } else {
            false
        }
    }

    ///Merges multiple line results to a single Vector
    pub fn merge_line_results(&self, key: Arc<String>) -> Vec<String> {
        //get a lock to the part
        let mut part_lock = self.parts.lock().unwrap();
        //get the entry to the key
        let entry = part_lock.get_mut(&key).unwrap();
        //combine all the values from the lines to a single vector
        let mut all_values = vec![];
        for val in entry.values_mut() {
            all_values.append(&mut val.1);
        }
        all_values
    }

    ///Saves a part result, returns true if all parts for a key are reduced
    pub fn save_part_result(&self, key: Arc<String>, result: String) -> bool {
        //get a lock to the key
        let mut key_lock = self.keys.lock().unwrap();
        //get the entry to the key
        let entry = key_lock.get_mut(&key).unwrap();
        //save our result
        entry.1.push(result);
        //decrement the counter of file parts we need to reduce
        if entry.0 > 0 {
            entry.0 -= 1;
        }
        //are all the parts done?
        if entry.0 == 0 {
            true
        } else {
            false
        }
    }

    ///Returns the final result for a key. Also removes the entries from the tracker
    pub fn get_and_clean_key_results(&self, key: Arc<String>) -> Vec<String> {
        //get a lock to the part
        let mut part_lock = self.parts.lock().unwrap();
        part_lock.remove_entry(&key).unwrap();
        //get a lock to the key
        let mut key_lock = self.keys.lock().unwrap();
        //get the entry to the key
        let (_, entry) = key_lock.remove_entry(&key).unwrap();
        entry.1
    }
}
