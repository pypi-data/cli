use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
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

pub async fn parse_python_files(dataset: &Path, git_repo: PathBuf, limit: Option<usize>) {
    let config = SessionConfig::from_env().unwrap();
    let batch_size = config.batch_size();
    info!("Reading {} (batch size {})", dataset.display(), batch_size);

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
    // println!("{}", row.into_data());
    // 30,404,066
    // let total_rows = ctx.table(&provider).await.unwrap().count().await.unwrap();
    info!("Total rows: {total_rows:?}");
    let total_chunks = (total_rows / batch_size as i64) as usize;

    let progress_bars = indicatif::MultiProgress::new();

    let total_chunks_pbar = progress_bars.add(indicatif::ProgressBar::new(total_chunks as u64));
    total_chunks_pbar.enable_steady_tick(Duration::from_secs(1));

    git2::opts::enable_caching(false);
    git2::opts::strict_hash_verification(false);

    let output_fields = Stats::arrow_fields();
    let struct_type = Arc::new(DataType::Struct(Fields::from(output_fields.clone())));

    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);

    ctx.register_udf(create_udf(
        "parse_python_file",
        vec![DataType::Binary],
        struct_type,
        Volatility::Immutable,
        make_scalar_function(move |args: &[ArrayRef]| {
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

            let oid_debug: Vec<_> = arg0.iter().flatten().flat_map(|b| {
                Oid::from_bytes(b)
            }).map(|oid| oid.to_string()).collect();

            info!("oids: {oid_debug:?}");

            let results = arg0.into_iter().progress_with(pbar).map(|v| {
                let oid = match v {
                    None => {
                        return None;
                    }
                    Some(v) => Oid::from_bytes(v).unwrap(),
                };
                debug!("Parsing {:?}", oid);
                parse_oid(oid, &odb)
            });
            let results_vec: Vec<_> = results.collect();
            total_chunks_pbar.inc(1);
            Ok(Arc::new(results_vec.to_struct_array()))
        }),
    ));

    let df = ctx
        .sql(
            r#"
        SELECT hash, parse_python_file(hash) as statements
        FROM input_dataset
    "#,
        )
        .await
        .unwrap()
        .limit(0, limit)
        .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
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

    let result = debug_span!("parse_module").in_scope(|| libcst_native::parse_module(&string, Some("utf-8")));

    match result {
        Ok(module) => {
            Some(walk_cst(module))
        }
        Err(e) => {
            debug!("Failed to parse module: {e}");
            None
        }
    }
}
