use rocksdb;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use anyhow::{Context, Result};
use super::map_container::MapContainer;
pub struct Index {
    db: rocksdb::DB,
    root_dir: PathBuf,
    total_keys: AtomicUsize,
}

impl Index {
    pub fn new(path: &PathBuf) -> Result<Index> {
        let root_dir = path.clone();
        let mut index_path = root_dir.clone();
        index_path.push("index");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open_default(&index_path).with_context(|| format!("Could not create index in: {}", index_path.display()))?;

        Ok(Index {db, root_dir, total_keys: AtomicUsize::new(0)})
    }

    pub fn merge(&self, map_results: &Arc<RwLock<HashMap<String, MapContainer>>>, flush_size: usize, max_part_size: usize) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut map_results = map_results.write().unwrap();
        for (key, mut memory_container) in map_results.drain() {
            match self.get(&key)? {
                Some(index_container) => {
                    let mut merged_container = MapContainer::new(&key);
                    merged_container.add_values(memory_container.values);
                    merged_container.transfer_data(index_container);
                    if merged_container.buffered_size >= flush_size {
                        merged_container.flush_to_file_part(&self.root_dir, max_part_size)?;
                        let bytes = MapContainer::serialize(&merged_container)?;
                        batch.put(&key, bytes);
                    } else {
                        let bytes = MapContainer::serialize(&merged_container)?;
                        batch.put(&key, bytes);
                    }
                },
                None => {
                    self.total_keys.fetch_add(1, Ordering::SeqCst);
                    if memory_container.buffered_size >= flush_size {
                        memory_container.flush_to_file_part(&self.root_dir, max_part_size)?;
                        let bytes = MapContainer::serialize(&memory_container)?;
                        batch.put(&key, bytes);
                    } else {
                        let bytes = MapContainer::serialize(&memory_container)?;
                        batch.put(&key, bytes);
                    }
                }
            }
        }
        self.db.write(batch).context("Could not write to index")?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<MapContainer>> {
        let container = match self.db.get(&key).context("Could not read from the index")? {
            Some(bytes) => Some(MapContainer::deserialize(&bytes)?),
            None => None
        };

        Ok(container)
    }

    #[allow(dead_code)]
    pub fn total_keys(&self) -> usize {
        self.total_keys.load(Ordering::SeqCst)
    }

    pub fn root(&self) -> PathBuf {
        self.root_dir.clone()
    }

    ///Creates an iterator over index entries
    pub fn iter(&self) -> IndexIterator {
        IndexIterator { iterator: self.db.iterator(rocksdb::IteratorMode::Start) }
    }
}

///Wrapper around the rocksdb iterator to create a higher level iterator that also deserializes the entries
pub struct IndexIterator<'r> {
    iterator: rocksdb::DBIterator<'r>
}

impl<'r> Iterator for IndexIterator<'r> {
    type Item = Result<(String, MapContainer)>;

    fn next(&mut self) -> Option<Result<(String, MapContainer)>> {
        let result = self.iterator.next();
        match result {
            Some((key, value)) => {
                let value = value.into_vec();
                let key = key.into_vec();
                let key = String::from_utf8(key).context("Could not parse index key");
                //turn (Result<key>, Result<value>) to Result<(key, value)>
                match key {
                    Err(err) => Some(Err(err)),
                    Ok(key) => {
                        let container = MapContainer::deserialize(&value);
                        match container {
                            Err(err) => Some(Err(err)),
                            Ok(container) => Some(Ok((key, container)))
                        }
                    }
                }
            },
            None => None
        }
    }
}
