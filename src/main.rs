use std::error::Error;
use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;
use polars::sql::SQLContext;

#[derive(Debug, Parser)]
#[command(name = "pcat")]
#[command(about = "cat for .parquet", long_about = None)]
struct Pcat {
    #[arg(required = true)]
    files: Vec<PathBuf>,
    #[arg(short, long, help = "SELECT `foo.bar`, `foo.baz` FROM t WHERE `foo.qux` <> 1337")]
    query: Option<String>,
}

type MainResult = Result<(), Box<dyn Error>>;
type LazyFrames = Result<Vec<LazyFrame>, PolarsError>;

fn main() -> MainResult {
    let args = Pcat::parse();
    let lfs = args.files.iter().map(get_parquet).collect::<LazyFrames>()?;
    let lfs = concat_lf_diagonal(lfs, UnionArgs::default())?;
    let lf = match args.query {
        Some(query) => {
            let mut context = SQLContext::new();
            context.register("t", lfs);
            context.execute(&query)?
        }
        _ => lfs,
    };
    let result = lf.collect()?;
    println!("{result:?}");
    Ok(())
}

fn get_parquet(path: &PathBuf) -> PolarsResult<LazyFrame> {
    let result = LazyFrame::scan_parquet(path, ScanArgsParquet::default());
    result.map_err(|e: PolarsError| {
        e.wrap_msg(&|msg| format!("Couldn't read {path:?} ({msg})"))
    })
}
