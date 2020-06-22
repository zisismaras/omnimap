use std::io::BufRead;
use anyhow::Result;
use std::sync::{mpsc::Sender, Arc};
use super::thread_pool::ThreadPool;
use super::indexer::IndexGuard;
use super::js::{MapResult, ContextBuilder};

///Reads from reader -> runs map -> sends results to the indexing channel
pub fn map<T: BufRead>(
    reader: &mut T,
    pool: ThreadPool,
    sender: Sender<Vec<MapResult>>,
    index_guard: IndexGuard,
    context_builder: Arc<ContextBuilder>,
    read_buffer_size: usize
) -> Result<()> {
    let mut buf = String::with_capacity(read_buffer_size);
    let mut current_line = 0;
    while reader.read_line(& mut buf)? > 0 {
        current_line += 1;
        if buf.len() >= read_buffer_size {
            let current_buf: String = buf.drain(..).collect();
            let context_builder = context_builder.clone();
            let sender = sender.clone();
            index_guard.wait_while_indexing();
            pool.execute(move|| {
                //create 1 js context per thread
                context_builder.reuse(|context| {
                    let result = context.run_map(current_line, &current_buf).unwrap();
                    sender.send(result).unwrap();
                });
            });
        }
    }
    //leftovers
    if buf.len() > 0 {
        let context_builder = context_builder.clone();
        pool.execute(move|| {
            //create js context
            let context = context_builder.build().unwrap();
            sender.send(context.run_map(current_line, &buf).unwrap()).unwrap();
        });
    }

    Ok(())
}