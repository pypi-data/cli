use std::path::PathBuf;

// use crate::unique::get_unique_python_files;
use clap::{Parser, Subcommand};
use crate::cst_walker::walk_cst;

// use crate::git_cat_file::read_stream;
use crate::unique::get_unique_python_files;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// mod git_cat_file;
// mod repos;
mod cst_walker;
mod parse;
mod unique;

// #[global_allocator]
// static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Update {
        // #[arg(short, long)]
        directory: PathBuf,
    },
    Parse {
        contents: PathBuf,
    },
    Unique {
        directory: PathBuf,
        output: PathBuf,
    },
    HandleFiles {
        directory: PathBuf,
        git_repo: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Parse { contents } => {
            let text = std::fs::read_to_string(&contents).unwrap();
            let parsed = libcst_native::parse_module(&text, Some("utf-8")).unwrap();
            let stats = walk_cst(parsed);
            println!("Stats: {stats:?}");
        }
        Command::Update { directory: _ } => {
            // println!("Updating {}", directory.display());
            // let mut repo = repos::init_repo(&directory);
            // let _names = repos::set_upstreams(&mut repo);
            // repos::fetch_trees(&mut repo);
        }
        Command::Unique { directory, output } => {
            get_unique_python_files(&directory, &output).await;
        }
        Command::HandleFiles {
            directory,
            git_repo,
        } => {
            crate::parse::parse_python_files(&directory, git_repo).await;
            // let mut input = std::io::stdin().lock();
            // let mut input = BufReader::new(File::open("/Users/tom/tmp/oids.txt").unwrap());
            // read_stream(&mut input);
        }
    }
}
