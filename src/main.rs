use std::{io, str};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::{Parser, Subcommand};
use git2::Oid;
use tokio::runtime::Builder;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

use crate::cst_walker::walk_cst;
use crate::parse::{parse_data, parse_oid, ParseType};

mod cst_walker;
mod line_endings;
mod parse;
mod stats;
/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,

    #[arg(long)]
    log_dir: Option<PathBuf>,

    #[arg(long, default_value = "info")]
    log_level: Level,
}

#[derive(Subcommand, Debug)]
enum Command {
    Parse {
        contents: PathBuf,
    },
    WalkCST {
        dataset: PathBuf,
        git_repo: PathBuf,

        #[arg(short, long)]
        limit: Option<usize>,
    },
    Grep {
        dataset: PathBuf,
        git_repo: PathBuf,

        #[arg(short, long)]
        pattern: String,

        #[arg(short, long)]
        limit: Option<usize>,
    },
    ParseOid {
        git_repo: PathBuf,
        oid: Oid,
    },
    ParseFile {},
    DebugAST {},
    GetOid {
        oid: String,
    },
}


fn main() -> anyhow::Result<()> {
    let runtime = Builder::new_multi_thread()
        .thread_stack_size(100 * 1024 * 1024)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("tokio-runtime-worker-{}", id)
        })
        .build()
        .unwrap();
    runtime.block_on(run_main())?;
    Ok(())
}

async fn run_main() -> anyhow::Result<()> {
    let args = Args::parse();

    let x = tokio::runtime::Handle::try_current().unwrap();
    // println!("{:?}", x.runtime_flavor());
    // panic!();
    let non_blocking = match args.log_dir {
        None => BoxMakeWriter::new(io::stdout),
        Some(dir) => {
            if !dir.exists() {
                std::fs::create_dir_all(&dir).unwrap();
            }
            let appender = tracing_appender::rolling::never(dir, "run.log");
            // tracing_appender::non_blocking(appender)
            BoxMakeWriter::new(appender)
        }
    };

    tracing_subscriber::fmt()
        .with_max_level(args.log_level)
        .with_writer(non_blocking)
        // Display source code file paths
        .with_file(false)
        // Display source code line numbers
        .with_line_number(false)
        // Display the thread ID an event was recorded on
        .with_thread_names(true)
        // Don't display the event's target (module path)
        .with_target(false)
        .with_span_events(FmtSpan::CLOSE)
        .finish()
        .init();
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    match args.command {
        Command::WalkCST {
            dataset,
            git_repo,
            limit,
        } => {
            crate::parse::parse_python_files(&dataset, git_repo, ParseType::CST, limit).await;
        }
        Command::Grep {
            dataset,
            git_repo,
            pattern,
            limit,
        } => {
            let pattern = regex::bytes::Regex::new(&pattern)?;
            crate::parse::parse_python_files(&dataset, git_repo, ParseType::Regex(pattern), limit)
                .await;
        }
        Command::Parse { contents } => {
            let contents = std::fs::read(contents).unwrap();
            let normalized = line_endings::normalize(&contents);
            let text = str::from_utf8(&normalized).unwrap();
            let parsed = libcst_native::parse_module(text, Some("utf-8")).unwrap();
            println!("CST: {parsed:#?}");
            let stats = walk_cst(parsed);
            println!("Stats: {stats:#?}");
        }
        Command::ParseOid { git_repo, oid } => {
            tokio::task::spawn_blocking(move || {
                let repo = git2::Repository::open(git_repo).unwrap();
                let odb = repo.odb().unwrap();
                let stats = parse_oid(oid, &odb);
                println!("{stats:#?}");
            })
                .await?;
        }
        Command::ParseFile {} => {
            tokio::task::spawn_blocking(move || {
                let mut data = vec![];
                io::copy(&mut io::stdin().lock(), &mut data).unwrap();
                info!("Read {} bytes from stdin", data.len());
                let stats = parse_data(&data);
                println!("{stats:#?}");
            })
                .await?;
        }
        Command::DebugAST {} => {
            let mut data = vec![];
            io::copy(&mut io::stdin().lock(), &mut data).unwrap();
            let module = libcst_native::parse_module(str::from_utf8(&data).unwrap(), None).unwrap();
            println!("{module:#?}");
        }
        Command::GetOid { oid } => {
            let data: Vec<u8> = serde_json::from_str(&oid).unwrap();
            let oid = git2::Oid::from_bytes(&data).unwrap();
            println!("{oid}");
        }
    }
    Ok(())
}
