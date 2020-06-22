use rocksdb;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Context, Result};

///A persistent table holding the reduction results
pub struct ResultTable {
    db: Arc<rocksdb::DB>
}

impl Clone for ResultTable {
    fn clone(&self) -> Self {
        ResultTable {
            db: self.db.clone()
        }
    }
}

impl ResultTable {
    ///Creates the table under path
    pub fn new(path: &PathBuf) -> Result<ResultTable> {
        let root_dir = path.clone();
        let mut index_path = root_dir.clone();
        index_path.push("results");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open_default(&index_path).with_context(|| format!("Could not create result table in: {}", index_path.display()))?;

        Ok(ResultTable {db: Arc::new(db)})
    }

    ///Adds a new entry to the table
    pub fn add(&self, key: &str, result: &str) -> Result<()> {
        self.db.put(key, result).context("Could not save result")
    }

    ///Creates an iterator over the table entries
    pub fn iter(&self, order: ResultsOrdering) -> ResultTableIterator {
        match order {
            ResultsOrdering::Asc => {
                ResultTableIterator { iterator: self.db.iterator(rocksdb::IteratorMode::Start) }
            },
            ResultsOrdering::Desc => {
                ResultTableIterator { iterator: self.db.iterator(rocksdb::IteratorMode::End) }
            }
        }
    }
}

pub enum ResultsOrdering {
    Asc,
    Desc
}

impl ResultsOrdering {
    pub fn new(order: &str) -> ResultsOrdering {
        if order == "desc" {
            ResultsOrdering::Desc
        } else {
            ResultsOrdering::Asc
        }
    }
}

///Wrapper around the rocksdb iterator to create a higher level iterator that also deserializes the entries
pub struct ResultTableIterator<'r> {
    iterator: rocksdb::DBIterator<'r>
}

impl<'r> Iterator for ResultTableIterator<'r> {
    type Item = (String, String);

    fn next(&mut self) -> Option<(String, String)> {
        let result = self.iterator.next();
        match result {
            Some((key, value)) => {
                let key = String::from_utf8_lossy(&key);
                let value = String::from_utf8_lossy(&value);
                Some((key.into_owned(), value.into_owned()))
            },
            None => None
        }
    }
}
