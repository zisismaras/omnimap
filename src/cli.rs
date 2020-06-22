use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::env::temp_dir;
use clap::{Arg, App};
use anyhow::{Context, Result, anyhow};

///CLI options passed by the user
pub struct CLIOptions {
    pub user_code: String,
    pub read_buffer_size: usize,
    pub key_flush_size: usize,
    pub max_file_part_size: usize,
    pub index_every: usize,
    pub workers: usize,
    pub order: String,
    pub temp_dir: PathBuf,
}

impl CLIOptions {
    ///Parses CLI options and applies defaults
    pub fn new() -> Result<CLIOptions> {
        let cmd = App::new("Omnimap")
            .version("0.1.0")
            .arg(Arg::with_name("code")
                .value_name("FILE")
                .help("The map/reduce javascript file")
                .required(true))
            .arg(Arg::with_name("read_buffer_size")
                .display_order(1)
                .long("read-buffer-size")
                .value_name("KILOBYTES")
                .default_value("512")
                .help("How many kb to read from input for each map task"))
            .arg(Arg::with_name("key_flush_size")
                .display_order(2)
                .long("key-flush-size")
                .value_name("KILOBYTES")
                .default_value("64")
                .help("Flush threshold for each map key"))
            .arg(Arg::with_name("max_file_part_size")
                .display_order(3)
                .long("max-file-part-size")
                .value_name("KILOBYTES")
                .default_value("2048")
                .help("Maximum file size for each flushed key"))
            .arg(Arg::with_name("index_every")
                .display_order(4)
                .long("index-every")
                .value_name("NUMBER")
                .default_value("100")
                .help("How many map tasks to run before indexing"))
            .arg(Arg::with_name("workers")
                .display_order(5)
                .long("workers")
                .value_name("NUMBER")
                .help("The number of worker threads to use [default: auto]"))
            .arg(Arg::with_name("order")
                .display_order(6)
                .long("order")
                .possible_value("asc")
                .possible_value("desc")
                .default_value("asc")
                .value_name("ORDERING")
                .help("Key ordering of the output"))
            .arg(Arg::with_name("temp_dir")
                .display_order(7)
                .long("temp-dir")
                .value_name("DIR")
                .help("Use a different temp dir [default: system tmp]"))
            .get_matches();
        //the following unwraps are safe since clap has already checked for required arguments and defaults
        let user_code_file = cmd.value_of("code").unwrap();
        let user_code = get_user_code(user_code_file).context("Could not read javascript file")?;

        let read_buffer_size = cmd.value_of("read_buffer_size").unwrap().parse::<usize>().context("Invalid read buffer size")?;
        if read_buffer_size == 0 { return Err(anyhow!("Invalid read buffer size")) };
        let read_buffer_size = 1024 * read_buffer_size;

        let key_flush_size = cmd.value_of("key_flush_size").unwrap().parse::<usize>().context("Invalid key flush size")?;
        if key_flush_size == 0 { return Err(anyhow!("Invalid key flush size")) };
        let key_flush_size = 1024 * key_flush_size;

        let max_file_part_size = cmd.value_of("max_file_part_size").unwrap().parse::<usize>().context("Invalid file part size")?;
        if max_file_part_size == 0 { return Err(anyhow!("Invalid file part size")) };
        let max_file_part_size = 1024 * max_file_part_size;

        let index_every = cmd.value_of("index_every").unwrap().parse::<usize>().context("Invalid index cycle")?;
        if index_every == 0 { return Err(anyhow!("Invalid index cycle")) };

        let workers = if cmd.is_present("workers") {
            cmd.value_of("workers").unwrap().parse::<usize>().context("Invalid worker count")?
        } else {
            num_cpus::get()
        };
        if workers == 0 { return Err(anyhow!("Invalid worker count")) };

        let order = cmd.value_of("order").unwrap().to_owned();

        let temp_dir = if cmd.is_present("temp_dir") {
            PathBuf::from(cmd.value_of("temp_dir").unwrap())
        } else {
            temp_dir()
        };

        Ok(CLIOptions {
            user_code,
            read_buffer_size,
            key_flush_size,
            max_file_part_size,
            index_every,
            workers,
            order,
            temp_dir
        })
    }
}

///loads the user's code file
fn get_user_code(file_path: &str) -> Result<String, io::Error> {
    let path = Path::new(file_path);
    fs::read_to_string(path)
}
