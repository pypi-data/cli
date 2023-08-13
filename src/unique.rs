use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_binary_array;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::Volatility;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::functions::make_scalar_function;
// use polars::prelude::*;
use datafusion::prelude::*;
use git2::Oid;

pub async fn get_unique_python_files(directory: &Path, output: &PathBuf) {
    let glob_expr = &format!("{}/*.parquet", directory.display());
    println!("Reading {}", glob_expr);
    let config = SessionConfig::from_env().unwrap();
    let ctx = SessionContext::with_config(config);

    ctx.register_udf(create_udf(
        "git_oid",
        vec![DataType::Binary],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        make_scalar_function(|args: &[ArrayRef]| {
            let arg0 = as_binary_array(&args[0]).unwrap();
            let x: Vec<_> = arg0
                .into_iter()
                .flatten()
                .map(|v| Oid::from_bytes(v).unwrap().to_string())
                .collect();
            Ok(Arc::new(StringArray::from(x)) as ArrayRef)
        }),
    ));

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

    let df = ctx.sql(include_str!("unique_files.sql")).await.unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(13).unwrap()))
        .build();

    df.write_parquet(
        &format!("{}.parquet", output.join("done").display()),
        Some(props),
    )
    .await
    .unwrap();
}
