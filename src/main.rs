use std::error::Error;
use std::path::PathBuf;

use clap::{Args, Parser};
use polars::prelude::*;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "git")]
#[command(about = "A fictional versioning CLI", long_about = None)]
struct Pcat {
    /// Stuff to add
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Pcat::parse();
    println!("{:?}", args.files);
    let mut lfs = vec![];
    for file in args.files {
        let result = LazyFrame::scan_parquet(file, ScanArgsParquet::default())?;
        lfs.push(result);
    }
    let df_vertical_concat = concat_lf_diagonal(
        lfs,
        UnionArgs::default(),
    )?.collect()?;
    let result = df_vertical_concat;
    println!("{result:?}");
    Ok(())
}
