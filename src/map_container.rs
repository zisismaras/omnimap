use std::path::{PathBuf, Path};
use std::fs::OpenOptions;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use bincode;
use anyhow::{Context, Result, anyhow};
use super::json_line::to_json_line;

///Contains values and metadata for a map key
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct MapContainer {
    pub values: Vec<String>,
    pub buffered_size: usize,
    encoded_key: String,
    last_part_size: usize,
    last_part_sequence: usize,
    lines_per_part: Vec<usize>,
    total_parts: usize
}

impl MapContainer {
    ///Creates a new empty container for a key
    pub fn new(key: &str) -> MapContainer {
        MapContainer {
            encoded_key: base64::encode(key),
            values: vec![],
            buffered_size: 0,
            last_part_size: 0,
            last_part_sequence: 0,
            lines_per_part: vec![],
            total_parts: 0
        }
    }

    ///Serializes a container to bytes
    pub fn serialize(container: &MapContainer) -> Result<Vec<u8>> {
        bincode::serialize(&container).context("Could not serialize container")
    }

    ///Deserializes a sequence of bytes to a container
    pub fn deserialize(bytes: &Vec<u8>) -> Result<MapContainer> {
        bincode::deserialize(bytes).context("Could not deserialize container")
    }

    ///Adds a value to the container while also incrementing the buffered_size
    pub fn add_value(&mut self, value: String) {
        self.buffered_size += value.len();
        self.values.push(value);
    }

    ///Adds multiple values to the container while also incrementing the buffered_size
    pub fn add_values(&mut self, values: Vec<String>) {
        for value in values {
            self.add_value(value);
        }
    }

    ///Transfers all data from another container 
    pub fn transfer_data(&mut self, other: MapContainer) {
        self.last_part_sequence = other.last_part_sequence;
        self.last_part_size = other.last_part_size;
        self.lines_per_part = other.lines_per_part;
        self.total_parts = other.total_parts;
        self.add_values(other.values);
    }

    ///An iterator for container parts that yields the part number
    pub fn parts(&self) -> Parts {
        Parts {current: 0, total: self.total_parts}
    }

    ///Returns an enum with possible locations of saved container data
    pub fn state(&self) -> ContainerState {
        if self.total_parts > 0 && self.values.len() > 0 {
            ContainerState::IndexAndFile
        } else if self.total_parts > 0 {
            ContainerState::FileOnly
        } else if self.values.len() > 0 {
            ContainerState:: IndexOnly
        } else {
            ContainerState::NoData
        }
    }

    ///Returns the line count for a part number, if the part does not exist an error will be returned
    pub fn part_line_count(&self, part: usize) -> Result<usize> {
        let line_count = self.lines_per_part.get(part);
        match line_count {
            Some(c) => Ok(c.clone()),
            None => Err(anyhow!("Part {} does not exist", part))
        }
    }

    ///Constructs a file path for a part number, if the part does not exist an error will be returned
    pub fn part_file_path(&self, dir: &PathBuf, part: usize) -> Result<String> {
        if part > self.last_part_sequence {
            Err(anyhow!("Part {} does not exist", part))
        } else {
            Ok(format!("{}/{}.map.{}.jsonl", dir.display(), &self.encoded_key, part))
        }
    }

    ///Flushes the indexed values to their own file while creating new file parts as needed based on max_part_size.
    pub fn flush_to_file_part(&mut self, directory: &PathBuf, max_part_size: usize) -> Result<()> {
        //serialize
        let json_line = to_json_line(&self.values);
        //create the file if needed and open it
        let mut file_path = self.part_file_path(directory, self.last_part_sequence)?;
        let mut file = if !Path::new(&file_path).exists() {
            self.lines_per_part.push(1);
            self.total_parts += 1;
            OpenOptions::new().create(true).append(true).open(&file_path).with_context(|| format!("Could not open file part: {}", file_path))?
        } else {
            //check size and use a new file part if needed
            if json_line.len() + self.last_part_size >= max_part_size {
                self.last_part_sequence += 1;
                self.last_part_size = 0;
                file_path = self.part_file_path(directory, self.last_part_sequence)?;
                self.lines_per_part.push(1);
                self.total_parts += 1;
                OpenOptions::new().create(true).append(true).open(&file_path).with_context(|| format!("Could not open file part: {}", file_path))?
            } else {
                self.lines_per_part[self.last_part_sequence] += 1;
                OpenOptions::new().append(true).open(&file_path).with_context(|| format!("Could not open file part: {}", file_path))?
            }
        };
        //write and reset
        file.write_all(&json_line.as_bytes()).with_context(|| format!("Could not write to file part: {}", file_path))?;
        file.sync_all().with_context(|| format!("Could not fsync file part: {}", file_path))?;
        self.last_part_size += json_line.len();
        self.values = Vec::new();
        self.buffered_size = 0;

        Ok(())
    }
}

pub struct Parts {
    current: usize,
    total: usize
}

impl Iterator for Parts {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        if self.total > 0 && self.current < self.total {
            self.current += 1;
            Some(self.current - 1) //we yield the part sequence which starts from 0
        } else {
            None
        }
    }
}

pub enum ContainerState {
    NoData,
    IndexOnly,
    FileOnly,
    IndexAndFile
}
