use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use super::{js::MapResult, map_container::MapContainer};

type Bucket = Arc<RwLock<HashMap<String, MapContainer>>>;
type BucketList = Vec<Bucket>;

///Combines the raw map results based on their key.  
pub fn combine_map_results(
    bucket_list: &mut BucketList,
    raw_results: Vec<MapResult>,
    partitions: usize
) {
    for r in raw_results {
        let bucket_index = calculate_hash(&r.key) as usize % partitions;
        let mut bucket = bucket_list[bucket_index].write().unwrap();
        match bucket.get_mut(&r.key) {
            Some(existing) => {
                existing.add_value(r.value);
            },
            None => {
                let mut container = MapContainer::new(&r.key);
                container.add_value(r.value);
                bucket.insert(r.key, container);
            }
        }
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
