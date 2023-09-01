use std::path::PathBuf;

use clap::Parser;
use tracing::Level;
use tracing_subscriber::prelude::*;

use crate::output::OutputMode;
use crate::parse::{parse_files, SearchType};

mod output;
mod parse;

// mod old;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,

    #[arg(long, default_value = "info")]
    log_level: Level,
}

#[derive(Parser, Debug)]
enum Command {
    #[command(about = "Parse files")]
    Parse {
        data_dir: PathBuf,
        pattern: glob::Pattern,

        #[arg(long, short)]
        contents: Option<regex::bytes::Regex>,
    },
    #[command(about = "Extract files")]
    Extract {
        data_dir: PathBuf,
        to_dir: PathBuf,
        pattern: glob::Pattern,

        #[arg(long, short)]
        contents: Option<regex::bytes::Regex>,
    },
}

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(args.log_level);

    match args.command {
        Command::Parse {
            data_dir,
            pattern,
            contents,
        } => {
            parse_files(
                data_dir,
                SearchType::Glob(pattern),
                contents,
                OutputMode::Json,
            )?;
        }
        Command::Extract {
            data_dir,
            to_dir,
            pattern,
            contents,
        } => {
            parse_files(
                data_dir,
                SearchType::Glob(pattern),
                contents,
                OutputMode::Directory(to_dir),
            )?;
        }
    }
    Ok(())
}

fn init_logging(level: Level) {
    // let current_dir = std::env::current_dir().unwrap();
    // let log_file_name = log_file.file_name().unwrap();
    // let log_dir = match log_file.parent() {
    //     Some(p) => p,
    //     None => current_dir.as_path(),
    // };
    // let log_path = dir.parent().unwrap();//.unwrap_or_else(|| std::env::current_dir().unwrap());
    // let appender = tracing_appender::rolling::never(log_dir, log_file_name);
    // let (_non_blocking_appender, guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt()
        .with_max_level(level)
        // .with_writer(non_blocking_appender)
        .with_writer(std::io::stderr)
        // Display source code file paths
        .with_file(false)
        // Display source code line numbers
        .with_line_number(false)
        // Display the thread ID an event was recorded on
        .with_thread_names(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // .with_span_events(FmtSpan::CLOSE)
        .finish()
        .init();
}
