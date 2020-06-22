use std::thread::{spawn, JoinHandle};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, mpsc::{Sender, channel}, Mutex, Condvar, Barrier};
use std::path::PathBuf;
use anyhow::Result;

use super::combiner::combine_map_results;
use super::js::MapResult;
use super::index::Index;
use super::thread_pool::ThreadPool;

///Creates the index on disk and spawns the indexer thread
pub fn spawn_indexer(
    index_dir: &PathBuf,
    pool: ThreadPool,
    partitions: usize,
    key_flush_size: usize,
    max_file_part_size: usize,
    index_every: usize
) -> Result<(JoinHandle<()>, Sender<Vec<MapResult>>, Arc<Index>, IndexGuard)> {
    let index = Arc::new(Index::new(index_dir)?);
    let thread_index = index.clone();
    let (sender, receiver) = channel();
    let index_guard = IndexGuard::new();
    let thread_index_guard = index_guard.clone();
    let handle = spawn(move|| {
        //setup the bucket list
        let mut bucket_list = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            bucket_list.push(Arc::new(RwLock::new(HashMap::new())));
        }
        let mut map_iterations: usize = 0;
        for results in receiver.iter() {
            map_iterations += 1;
            combine_map_results(&mut bucket_list, results, partitions);
            if map_iterations >= index_every {
                let active_buckets = bucket_list.iter().filter(|b| b.read().unwrap().len() > 0);
                let b = Arc::new(Barrier::new(active_buckets.clone().count() + 1));
                thread_index_guard.start_indexing();
                map_iterations = 0;
                for bucket in active_buckets {
                    let index = thread_index.clone();
                    let bucket = Arc::clone(bucket);
                    let b = b.clone();
                    pool.execute(move|| {
                        index.merge(&bucket, key_flush_size, max_file_part_size).unwrap();
                        b.wait();
                    });
                }
                b.wait();
                thread_index_guard.finish_indexing();
            }
        }
        //do a last index
        let active_buckets = bucket_list.iter().filter(|b| b.read().unwrap().len() > 0);
        for bucket in active_buckets {
            let index = thread_index.clone();
            let bucket = Arc::clone(bucket);
            pool.execute(move|| {
                index.merge(&bucket, key_flush_size, max_file_part_size).unwrap();
            });
        }
    });

    Ok((handle, sender, index, index_guard))
}

pub struct IndexGuard {
    guard: Arc<(Mutex<bool>, Condvar)>
}

impl Clone for IndexGuard {
    fn clone(&self) -> IndexGuard {
        IndexGuard {
            guard: self.guard.clone()
        }
    }
}

impl IndexGuard {
    pub fn new() -> IndexGuard {
        IndexGuard {
            guard: Arc::new((Mutex::new(false), Condvar::new()))
        }
    }

    pub fn start_indexing(&self) {
        let (lock, cvar) = &*self.guard;
        let mut indexing = lock.lock().unwrap();
        *indexing = true;
        cvar.notify_one();
    }

    pub fn finish_indexing(&self) {
        let (lock, cvar) = &*self.guard;
        let mut indexing = lock.lock().unwrap();
        *indexing = false;
        cvar.notify_one();
    }

    pub fn wait_while_indexing(&self) {
        let (lock, cvar) = &*self.guard;
        let _guard = cvar.wait_while(
            lock.lock().unwrap(),
            |indexing| {
                *indexing
            }
        ).unwrap();
    }
}
