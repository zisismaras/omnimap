///Serializes to a jsonline string.  
///http://jsonlines.org/
pub fn to_json_line<T: serde::Serialize>(values: &T) -> String {
    let mut json_line = serde_json::json!(values).to_string();
    json_line.push_str("\n");
    json_line
}

use anyhow::{Result, Context};

///Deserializes from a json string.  
pub fn from_json<'a, T: serde::Deserialize<'a>>(line: &'a str) -> Result<T> {
    serde_json::from_str(line).with_context(|| format!("Could not parse json: {}", line))
}