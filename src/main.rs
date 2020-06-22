use std::io::{stdin, stdout};
use std::path::PathBuf;
use std::fs::{create_dir_all, remove_dir_all};
use std::sync::Arc;
use uuid::Uuid;
use anyhow::{Context, Result};

mod thread_pool;
use thread_pool::ThreadPool;
mod combiner;
mod js;
mod map_container;
mod json_line;
mod index;
mod cli;
use cli::CLIOptions;
mod indexer;
use indexer::spawn_indexer;
mod mapper;
use mapper::map;
mod reducer;
use reducer::spawn_reducer;
mod consumer;
use consumer::spawn_consumer;
mod printer;
use printer::print;
mod result_table;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    //get CLI options
    let options = CLIOptions::new()?;

    let context_builder = js::ContextBuilder::new(&options.user_code);
    let context_builder = Arc::new(context_builder);

    //create a js context for testing
    //so we can fail quickly if the js file has any errors before doing any more work
    {
        let context = context_builder.build()?;
        context.validate()?;
    }

    let dir = create_temp_dir(options.temp_dir)?;
    let pool = ThreadPool::new(options.workers);

    //spawn the indexer
    //get back a channel sender for mapper->indexer and the index
    let (indexer, sender, index, index_guard) = spawn_indexer(
        &dir,
        pool.clone(), 
        options.workers, 
        options.key_flush_size,
        options.max_file_part_size,
        options.index_every
    )?;

    //read and map
    map(
        &mut stdin().lock(),
        pool.clone(),
        sender,
        index_guard,
        context_builder.clone(),
        options.read_buffer_size
    )?;

    //wait for indexing to finish
    indexer.join().unwrap();
    pool.join();

    //spawn the reducer
    //get back a channel sender for consumer->reducer and the result_table
    let (reducer, sender, result_table) = spawn_reducer(
        pool.clone(),
        context_builder.clone(),
        options.workers,
        &dir
    )?;

    //spawn the consumer of the index
    let consumer = spawn_consumer(index, sender, options.key_flush_size);

    //wait for everything to finish
    consumer.join().unwrap()?;
    reducer.join().unwrap();
    pool.join();

    //write the reducer results
    print(&mut stdout().lock(), &result_table, &options.order)?;

    //clean up
    remove_temp_dir(dir)?;

    Ok(())
}

fn create_temp_dir(root: PathBuf) -> Result<PathBuf> {
    let mut dir = root.clone();
    let uuid = Uuid::new_v4();
    dir.push(format!("omnimap-{}", uuid));
    create_dir_all(&dir).with_context(|| format!("Could not create temp directory: {}", dir.display()))?;

    Ok(dir)
}

fn remove_temp_dir(dir: PathBuf) -> Result<()> {
    if dir.exists() {
        remove_dir_all(&dir).with_context(|| format!("Could not remove temp directory: {}", dir.display()))?;
    }
    Ok(())
}