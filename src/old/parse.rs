use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Fields, Int64Type};
use datafusion::common::cast::as_binary_array;
use datafusion::logical_expr::Volatility;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::*;
use git2::Oid;
use indicatif::ProgressIterator;
use tracing::{debug, debug_span, info};

use crate::cst_walker::walk_cst;
use crate::line_endings;
use crate::stats::{Stats, ToStructArray};

#[derive(Debug, Clone)]
pub enum ParseType {
    CST,
    Regex(regex::bytes::Regex),
}

pub async fn parse_python_files(
    dataset: &Path,
    git_repo: PathBuf,
    parse_type: ParseType,
    limit: Option<usize>,
) {
    // let config = SessionConfig::from_env().unwrap();
    let config = SessionConfig::default();
    let batch_size = config.batch_size();
    let target_partitions = config.target_partitions();
    info!(
        "Reading {} (batch size {}, target partitions {})",
        dataset.display(),
        batch_size,
        target_partitions
    );

    let ctx = SessionContext::with_config(config);
    let read_options = ParquetReadOptions::default().parquet_pruning(true);
    ctx.register_parquet("input_dataset", dataset.to_str().unwrap(), read_options)
        .await
        .unwrap();

    let total_rows_result = ctx
        .sql("select count(*) as total from input_dataset")
        .await
        .unwrap()
        .limit(0, limit)
        .unwrap()
        .collect()
        .await
        .unwrap();
    let row = total_rows_result[0].column(0);
    let primitive: &PrimitiveArray<Int64Type> = row.as_primitive();
    let total_rows = primitive.into_iter().next().unwrap().unwrap();

    info!("Total rows: {total_rows:?}");
    let total_chunks = (total_rows / batch_size as i64) as usize;

    let progress_bars = indicatif::MultiProgress::new();

    let total_chunks_pbar = progress_bars.add(indicatif::ProgressBar::new(total_chunks as u64));
    total_chunks_pbar.enable_steady_tick(Duration::from_secs(1));

    git2::opts::enable_caching(false);
    git2::opts::strict_hash_verification(false);

    let result_type = match parse_type {
        ParseType::CST => Arc::new(DataType::Struct(Fields::from(Stats::arrow_fields()))),
        ParseType::Regex(_) => Arc::new(DataType::Boolean),
    };
    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);

    let func = create_udf(
        "parse_python_file",
        vec![DataType::Binary],
        result_type,
        Volatility::Immutable,
        make_scalar_function(move |args: &[ArrayRef]| {
            // total_chunks_pbar.inc(1);
            // return Ok(Arc::new(BooleanArray::from(vec![true; args[0].len()])));
            // info!("IN UDF {}", args[0].len());
            let repo = git2::Repository::open(git_repo.clone()).unwrap();
            let odb = repo.odb().unwrap();

            let arg0 = as_binary_array(&args[0]).unwrap();

            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);

            let pbar = progress_bars.add(indicatif::ProgressBar::new(arg0.len() as u64));
            pbar.set_style(
                indicatif::ProgressStyle::with_template(
                    "{msg}: [{elapsed_precise}] [{bar:40.cyan/blue}] {per_sec} p/s ({pos}/{len}, ETA {eta})",
                )
                    .unwrap(),
            );
            pbar.enable_steady_tick(Duration::from_secs(1));
            pbar.set_message(format!("Chunk {id}/{total_chunks}"));

            let oid_debug: Vec<_> = arg0
                .iter()
                .flatten()
                .flat_map(|b| Oid::from_bytes(b))
                .map(|oid| oid.to_string())
                .collect();

            // info!("oids: {oid_debug:?}");

            let parsed_oids = arg0.into_iter().progress_with(pbar).map(|v| {
                let oid = match v {
                    None => {
                        return None;
                    }
                    Some(v) => match Oid::from_bytes(v) {
                        Ok(oid) => oid,
                        Err(_) => {
                            return None;
                        }
                    },
                };
                Some(oid)
            });

            let parse_type_cloned = parse_type.clone();

            match parse_type_cloned {
                ParseType::CST => {
                    let results = parsed_oids.map(|oid| match oid {
                        None => None,
                        Some(oid) => {
                            debug!("Parsing {:?}", oid);
                            parse_oid(oid, &odb)
                        }
                    });
                    let results_vec: Vec<_> = results.collect();
                    total_chunks_pbar.inc(1);
                    info!("Done {id}/{total_chunks}");
                    Ok(Arc::new(results_vec.to_struct_array()))
                }
                ParseType::Regex(r) => {
                    let results = parsed_oids.map(|oid| match oid {
                        None => false,
                        Some(oid) => {
                            let binding = odb.read(oid);
                            let data = match &binding {
                                Ok(d) => d.data(),
                                Err(e) => {
                                    return false;
                                }
                            };
                            let is_match = r.is_match(data);
                            is_match
                        }
                    });
                    let results_vec: Vec<_> = results.collect();
                    total_chunks_pbar.inc(1);
                    info!("Done {id}/{total_chunks}");
                    Ok(Arc::new(BooleanArray::from(results_vec)))
                }
            }
        }),
    );

    // ctx.register_udf(func);

    let df = ctx
        .read_parquet(dataset.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .unwrap();
    let df = df
        .select(vec![col("hash"), func.call(vec![col("hash")])])
        .unwrap();
    // let df = df.limit(0, limit).unwrap();
    // let plan = df.create_physical_plan().await.unwrap();
    // let output = plan.output_partitioning().partition_count();
    // info!("{output:?}");
    // let df = ctx
    //     .sql(
    //         r#"
    //     SELECT hash, parse_python_file(hash) as stats
    //     FROM input_dataset
    // "#,
    //     )
    //     .await
    //     .unwrap()
    //     .limit(0, limit)
    //     .unwrap();

    let props = WriterProperties::builder()
        //     .set_compression(Compression::SNAPPY)
        .build();
    df.write_parquet("data/omg/", Some(props)).await.unwrap();
}

#[tracing::instrument(skip(odb), level = "debug")]
pub fn parse_oid(oid: Oid, odb: &git2::Odb) -> Option<Stats> {
    // info!("Parsing {:?}", oid);
    let binding = odb.read(oid);
    let data = match &binding {
        Ok(d) => d.data(),
        Err(_e) => {
            // info!("Failed to read from odb: {e}");
            return None;
        }
    };
    parse_data(data)
}

#[tracing::instrument(skip(data), level = "debug")]
pub fn parse_data(data: &[u8]) -> Option<Stats> {
    let normalized = line_endings::normalize(data);
    let string = match std::str::from_utf8(&normalized) {
        Ok(s) => s,
        Err(e) => {
            debug!("Failed to decode content: {e}");
            return None;
        }
    };
    debug!("Normalized and parsed UTF-8, parsing module");

    // These files cause stack overflows. Need to work around this in some other way.
    if string.starts_with("#  MINLP written by GAMS") {
        return None;
    }

    let result = debug_span!("parse_module").in_scope(|| libcst_native::parse_module(string, None));

    match result {
        Ok(module) => Some(walk_cst(module)),
        Err(e) => {
            debug!("Failed to parse module: {e}");
            None
        }
    }
}
