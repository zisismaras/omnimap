use std::thread::{spawn, JoinHandle};
use std::io::{BufReader, prelude::*};
use std::fs::File;
use std::sync::{Arc, mpsc::SyncSender};
use std::path::Path;
use anyhow::{Result, anyhow};
use super::map_container::ContainerState;
use super::reducer::{Reduction, ReduceValue};
use super::index::Index;

pub fn spawn_consumer(index: Arc<Index>, sender: SyncSender<Reduction>, flush_size: usize) -> JoinHandle<Result<()>> {
    let consumer = spawn(move|| -> Result<()> {
        let mut line_buffer = String::with_capacity(flush_size);
        for pair in index.iter() {
            let (key, container) = pair?;
            let key = Arc::new(key);
            let total_parts = container.parts().count();
            match container.state() {
                ContainerState::IndexAndFile => {
                    sender.send(Reduction::KeyInit(key.clone(), total_parts + 1))?;
                    sender.send(Reduction::FilePartInit(key.clone()))?;
                    for part in container.parts() {
                        let file_path = container.part_file_path(&index.root(), part)?;
                        if !Path::new(&file_path).exists() {
                            return Err(anyhow!("Temp directory modified while running"));
                        }
                        sender.send(Reduction::FileLineInit(key.clone(), part, container.part_line_count(part)?))?;
                        let mut reader = BufReader::new(File::open(&file_path)?);
                        while reader.read_line(&mut line_buffer)? > 0 {
                            sender.send(Reduction::FileLine(key.clone(), part, ReduceValue::FromFile(line_buffer.drain(..).collect())))?;
                        }
                    }
                    //index values are treated as a new file part with only 1 line
                    let new_part = container.parts().last().unwrap() + 1;
                    sender.send(Reduction::FileLineInit(key.clone(), new_part, 1))?;
                    sender.send(Reduction::FileLine(key.clone(), new_part, ReduceValue::FromIndex(container.values)))?;
                },
                ContainerState::FileOnly => {
                    sender.send(Reduction::KeyInit(key.clone(), total_parts))?;
                    sender.send(Reduction::FilePartInit(key.clone()))?;
                    for part in container.parts() {
                        let file_path = container.part_file_path(&index.root(), part)?;
                        if !Path::new(&file_path).exists() {
                            return Err(anyhow!("Temp directory modified while running"));
                        }
                        sender.send(Reduction::FileLineInit(key.clone(), part, container.part_line_count(part)?))?;
                        let mut reader = BufReader::new(File::open(&file_path)?);
                        while reader.read_line(&mut line_buffer)? > 0 {
                            sender.send(Reduction::FileLine(key.clone(), part, ReduceValue::FromFile(line_buffer.drain(..).collect())))?;
                        }
                    }
                },
                ContainerState::IndexOnly => {
                    //index values are treated as a single file part with only 1 line
                    sender.send(Reduction::KeyInit(key.clone(), 1))?;
                    sender.send(Reduction::FilePartInit(key.clone()))?;
                    sender.send(Reduction::FileLineInit(key.clone(), 0, 1))?;
                    sender.send(Reduction::FileLine(key.clone(), 0, ReduceValue::FromIndex(container.values)))?;
                },
                ContainerState::NoData => {
                    continue;
                }
            }
        }
        Ok(())
    });
    consumer
}