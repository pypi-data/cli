use std::time::Duration;

use indicatif::{ParallelProgressIterator, ProgressIterator};
use libcst_native::{TokConfig, TokType, TokenIterator};
use polars::prelude::*;
use rayon::prelude::*;

pub fn read_stream(_stream: &mut (impl std::io::BufRead + Send)) {
    // let targets = ["async", "class"];
    // let _discriminants: Vec<_> = targets
    //     .into_iter()
    //     .map(|t| {
    //         let mut target_lexr = lex(t, Mode::Module); //lex::make_tokenizer_located(t, Location::new(1, 0));
    //         let x = target_lexr.collect_vec();
    //         println!("{t} = {:?}", x);
    //         // let (target_token, _) = target_lexr.next().unwrap().unwrap();
    //         // std::mem::discriminant(&target_token)
    //         1
    //     })
    //     .collect();
    let mut file = std::fs::File::open("/Users/tom/tmp/unique_hashes.parquet").unwrap();
    let df = ParquetReader::new(&mut file).finish().unwrap();
    let mut df = df.sort(["repository"], false, true).unwrap();
    let repos = df.partition_by(["repository"], true).unwrap();
    // let col = df.column("hash").unwrap();

    let m = indicatif::MultiProgress::new();

    let chunks_pb = m.add(indicatif::ProgressBar::new(repos.len() as u64));
    chunks_pb.set_style(
        indicatif::ProgressStyle::with_template(
            "Chunks: [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
        )
        .unwrap(),
    );
    chunks_pb.enable_steady_tick(Duration::from_secs(1));

    // let all_pb = m.add(indicatif::ProgressBar::new(col.len() as u64));
    // all_pb.set_style(
    //     indicatif::ProgressStyle::with_template(
    //         "All Files: [{elapsed_precise}] [{bar:40.cyan/blue}] ({per_sec} p/s - {pos}/{len}, ETA {eta})",
    //     )
    //         .unwrap(),
    // );
    // all_pb.enable_steady_tick(Duration::from_secs(1));

    // let chunks: Vec<_> = col.chunks().iter().map(|c| c.to_owned()).collect();
    // let foo = &col.chunks().iter().map(|x| x.as_ref()).chunks(4);
    // let chunks = col.chunks().iter().map(|x| x.to_owned()).chunks(1).into_iter().map(|chunk| {
    //     arrow_concatenate(&chunk.map(|x|&x).collect_vec()).unwrap()
    // }).collect_vec();
    // let chunks: Vec<_> = col.chunks().iter().map(|c| c.to_owned()).collect();
    // let all = arrow_concatenate(&col.chunks().iter().map(|x| x.as_ref()).chunks(4).into_iter().map(|c| c.collect_vec()).collect_vec()).unwrap();

    git2::opts::enable_caching(false);
    git2::opts::strict_hash_verification(false);

    // let x = col.binary().unwrap();
    let total: usize = repos
        .into_par_iter()
        // .into_iter()
        .progress_with(chunks_pb)
        .map(|chunk| {
            let repo = git2::Repository::open("/Users/tom/tmp/git_repos/").unwrap();
            let odb = repo.odb().unwrap();
            let series = chunk.column("hash").unwrap();
            // let series = Series::try_from(("x", chunk)).unwrap();
            let pb = m.add(indicatif::ProgressBar::new(series.len() as u64));
            let thread_id = rayon::current_thread_index().unwrap_or_default();
            let tmpl =
                "[{elapsed_precise}] [{bar:40.cyan/blue}] ({per_sec} p/s - {pos}/{len}, ETA {eta})";

            pb.set_style(
                indicatif::ProgressStyle::with_template(&format!("Thread {thread_id}: {tmpl}"))
                    .unwrap(),
            );
            pb.enable_steady_tick(Duration::from_secs(1));
            // let mut buffer = Vec::with_capacity(1024 * 1024 * 5);
            let total: usize = series
                .binary()
                .unwrap()
                .into_iter()
                .progress_with(pb)
                .map(|oid| {
                    let oid = match oid {
                        None => return 0,
                        Some(r) => r,
                    };
                    // buffer.clear();
                    let oid = git2::Oid::from_bytes(oid).unwrap();
                    // let (mut reader, _, _) = match odb.reader(oid) {
                    //     Ok(v) => v,
                    //     Err(e) => {
                    //         eprintln!("error reading oid: {e}");
                    //         return 0;
                    //     }
                    // };
                    // io::copy(&mut reader, &mut buffer).unwrap();
                    // let buffer = odb.read(oid).unwrap();
                    let contents = odb.read(oid).unwrap();
                    // let source = String::from_utf8_lossy(contents.data());
                    match simdutf8::basic::from_utf8(&contents.data()) {
                        Ok(source) => {
                            let iter = TokenIterator::new(
                                source,
                                &TokConfig {
                                    async_hacks: false,
                                    split_fstring: false,
                                },
                            );
                            let any = iter.into_iter().any(|t| match t {
                                Ok(libcst_native::Token {
                                    r#type: TokType::Async,
                                    ..
                                }) => true,
                                _ => false,
                            });
                            match any {
                                true => 0,
                                false => 0,
                            }
                        }
                        Err(_) => 0,
                    }
                })
                .sum();
            total
        })
        .sum();
    println!("total: {total}");
    // for chunk in col.chunks() {
    //     println!("{}", chunk.len());
    // }

    // let df = DataFrame:("/Users/tom/tmp/unique_hashes.parquet", Default::default()).unwrap();
    // df.
    // let repo = git2::Repository::open("/Users/tom/tmp/git_repos/").unwrap();
    // let odb = repo.odb().unwrap();
    // let mut buffer = String::new();
    // let mut content_buffer = vec![0; 1024 * 1024 * 10];

    // let total: usize = stream.lines().progress_with(pb).par_bridge().map(|line| {
    //     // let odb = &x.odb;
    //     // let oid = line.unwrap();
    //     // let oid = git2::Oid::from_str(&oid).unwrap();
    //     // match odb.read_header(oid) {
    //     //     Ok((s, _)) => s,
    //     //     Err(_) => {
    //     //         0
    //     //     }
    //     // }
    //     line.unwrap().len()
    // }).sum();
    // println!("Total: {total}");
    // for line in stream.lines() {
    //     let oid = line.unwrap();
    //     let oid = git2::Oid::from_str(&oid).unwrap();
    //     let (size, _) = match odb.read_header(oid) {
    //         Ok(r) => r,
    //         Err(_) => {
    //             continue
    //         }
    //     };
    //     total += size;
    // }
    // println!("{total}");
    // while let Ok((_oid, _result)) = read_oid_line(stream, &mut buffer, &mut content_buffer) {
    // let source = match String::from_utf8(result) {
    //     Ok(s) => s,
    //     Err(_) => continue,
    // };
    // let mut output = vec![0; targets.len()];
    // let lxr = lexer::make_tokenizer_located(&source, Location::new(1, 0));
    // println!("{}", lxr.count());
    // for token in lxr {
    //     let (_, token, _) = match token {
    //         Ok(t) => t,
    //         Err(_) => continue,
    //     };
    // let discriminator = std::mem::discriminant(&token);
    // for (idx, target) in discriminants.iter().enumerate() {
    //     if target == &discriminator {
    //         output[idx] += 1;
    //     }
    // }
    // }
    // print!("{oid} ");
    // for item in output {
    //     print!("{item} ");
    // }
    // print!("\n");
    // buffer.clear();
    // println!("{}", content_buffer.len());
    // }
}

// pub fn read_oid_line<'a>(
//     stream: &'a mut impl std::io::BufRead,
//     buffer: &'a mut String,
//     content_buf: &'a mut [u8],
// ) -> Result<(&'a str, usize)> {
//     stream.read_line(buffer)?;
//     let mut split = buffer.split(' ');
//     let oid = split.next().ok_or_else(|| anyhow!("Invalid oid"))?;
//     let length = split.nth(1).ok_or_else(|| anyhow!("Invalid length"))?;
//     let length = length[..length.len() - 1].parse::<usize>()?;
//     // println!("Length: {length:?}");
//     // let mut buffer = vec![0u8; length + 1];
//     // let mut buffer = Vec::with_capacity(length);
//     stream.read_exact(&mut content_buf[0..length + 1])?;
//     // Read a newline if possible
//     // let mut newline = [0u8; 1];
//     // stream.read_exact(&mut newline)?;
//     Ok((oid, length))
// }
