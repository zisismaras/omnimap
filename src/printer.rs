use std::io::Write;
use anyhow::Result;
use super::result_table::{ResultTable, ResultsOrdering};

///Writes the entries in ResultTable to the writer in the format of "key\tvalue\n"
pub fn print<T: Write>(writer: &mut T, result_table: &ResultTable, order: &str) -> Result<()> {
    for (key, result) in result_table.iter(ResultsOrdering::new(order)) {
        writer.write_all(format!("{}\t{}\n", key, result).as_bytes())?;
    }
    Ok(())
}