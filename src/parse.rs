use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, FixedSizeListBuilder, PrimitiveArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::common::cast::as_binary_array;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::Volatility;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
// use datafusion::parquet::basic::{Compression, ZstdLevel};
// use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::functions::make_scalar_function;
// use polars::prelude::*;
use crate::cst_walker::walk_cst;
use datafusion::prelude::*;
use git2::Oid;
use libcst_native::TokType;
// use tokio_stream::StreamExt;

pub async fn parse_python_files(directory: &Path, git_repo: PathBuf) {
    let glob_expr = &format!("{}/*.parquet", directory.display());
    let config = SessionConfig::from_env().unwrap(); //.with_batch_size(1000);
    println!("Reading {} (batch size {})", glob_expr, config.batch_size());

    let ctx = SessionContext::with_config(config);

    let session_state = ctx.state();
    let table_path = ListingTableUrl::parse(directory.to_str().unwrap()).unwrap();
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");
    let resolved_schema = listing_options
        .infer_schema(&session_state, &table_path)
        .await
        .unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(config).unwrap());

    ctx.register_table("input_dataset", provider).unwrap();

    let total_rows_result = ctx
        .sql("select count(*) as total from input_dataset")
        .await
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
    println!("Total rows: {total_rows:?}");

    let pbar = indicatif::ProgressBar::new(total_rows as u64);
    pbar.set_style(
        indicatif::ProgressStyle::with_template(
            "Chunks: [{elapsed_precise}] [{bar:40.cyan/blue}] {per_sec} p/s ({pos}/{len}, ETA {eta})",
        )
            .unwrap(),
    );
    pbar.enable_steady_tick(Duration::from_secs(1));

    // let x = DataType::FixedSizeList(Box::new(Field::new("results", DataType::Boolean, false)), 4);

    let results_type = Arc::new(DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Boolean, true)),
        4,
    ));

    // let struct_type = Arc::new(DataType::Struct(Fields::from(vec![
    //     Field::new("has_async", DataType::Boolean, false),
    //     Field::new("has_fstring", DataType::Boolean, false),
    //     Field::new("has_walrus", DataType::Boolean, false),
    //     Field::new("has_matrix", DataType::Boolean, false),
    // ])));

    git2::opts::enable_caching(false);
    git2::opts::strict_hash_verification(false);

    ctx.register_udf(create_udf(
        "parse_python_file",
        vec![DataType::Binary],
        results_type,
        Volatility::Immutable,
        make_scalar_function(move |args: &[ArrayRef]| {
            let repo = git2::Repository::open(git_repo.clone()).unwrap();
            let odb = repo.odb().unwrap();

            let arg0 = as_binary_array(&args[0]).unwrap();
            let start_length = arg0.len();
            let results = arg0.into_iter().map(|v| {
                let oid = match v {
                    None => {
                        return None;
                    }
                    Some(v) => Oid::from_bytes(v).unwrap(),
                };
                let binding = odb.read(oid);
                let data = match &binding {
                    Ok(d) => d.data(),
                    Err(_) => return None,
                };
                let string = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => {
                        return None;
                    }
                };
                let parsed = match libcst_native::parse_module(string, Some("utf-8")) {
                    Ok(module) => {
                        walk_cst(module);
                    }
                    Err(_) => {
                        return None;
                    }
                };
                // let iter = libcst_native::CheapTokenIterator::new(
                //     string,
                //     &libcst_native::TokConfig {
                //         async_hacks: false,
                //         split_fstring: true,
                //     },
                // );
                // let items: Vec<_> = iter.collect();
                // let has_async = items
                //     .iter()
                //     .any(|i| matches!(i, Ok((TokType::Async | TokType::Await, _))));
                // let has_fstring = items
                //     .iter()
                //     .any(|i| matches!(i, Ok((TokType::FStringStart, _))));
                // let has_walrus = items.iter().any(|i| matches!(i, Ok((TokType::Op, ":="))));
                // let has_matrix = items.iter().any(|i| matches!(i, Ok((TokType::Op, "@"))));
                Some([false, false, false, false])
            });
            let mut builder =
                FixedSizeListBuilder::new(BooleanBuilder::with_capacity(start_length), 4);
            for item in results {
                match item {
                    Some(v) => {
                        builder.values().append_slice(&v);
                        builder.append(true);
                    }
                    None => {
                        builder.values().append_nulls(4);
                        builder.append(true);
                    }
                }
            }
            let result = builder.finish();
            pbar.inc(start_length as u64);
            Ok(Arc::new(result) as ArrayRef)
            // Ok(Arc::new(BooleanArray::from_iter(x)) as ArrayRef)
            // unreachable!();
            // let list_array = FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(x, 4);

            // let omg = FixedSizeListArray::from_iter_primitive::<BooleanType>(x, 4);
            // Ok(list_array.slice(0, list_array.len()))
            // Ok(FixedSizeListArray::from_iter_primitive([], x.len() as i32) as ArrayRef)
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
        .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    df.write_parquet("data/omg/", Some(props)).await.unwrap();

    //
    // // let mut stream = df.execute_stream().await.unwrap();
    // let mut streams = df.execute_stream_partitioned().await.unwrap();
    // //
    // let mut map = tokio_stream::StreamMap::new();
    // for (idx, s) in streams.into_iter().enumerate() {
    //     map.insert(idx, s);
    // }
    //
    // while let Some((idx, res)) = map.next().await {
    // // while let Some(res) = stream.next().await {
    //     let total_rows = res.map(|item| item.num_rows()).unwrap_or(0);
    //     pbar.inc(total_rows as u64);
    //     // println!("GOT = {}", total_rows);
    // }
    // let batches = df.collect().await.unwrap();
    // let output = pretty_format_batches(&batches).unwrap();
    // println!("{}", output);
    //
    // let df = ctx.sql(include_str!("unique_files.sql")).await.unwrap();
    //
    // let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(13).unwrap())).build();
    //
    // df.write_parquet(&format!("{}.parquet", output.join("done").display()), Some(props)).await.unwrap();
}
