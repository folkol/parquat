#![feature(unix_sigpipe)]

use std::error::Error;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;
use polars::sql::SQLContext;

/// cat for .parquet
#[derive(Debug, Parser)]
#[command(name = "pcat")]
#[command(version)]
struct Pcat {
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Sprinkle some SQL on the concatenated data frame (`t`).
    ///
    /// Example: "SELECT `foo.bar`, `foo.baz` FROM t WHERE `foo.qux` <> 1337"
    #[arg(short, long)]
    query: Option<String>,

    /// Show full output (do not condense)
    #[arg(short, long)]
    full: bool,

    /// Hide header (names and types)
    #[arg(short, long)]
    no_header: bool,
}

type MainResult = Result<(), Box<dyn Error>>;
type LazyFrames = Result<Vec<LazyFrame>, PolarsError>;

#[unix_sigpipe = "sig_dfl"]
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

    if args.no_header {
        std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_NAMES", "1");
        std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
    }

    if args.full || !std::io::stdout().is_terminal() {
        std::env::set_var("POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION", "1");
        std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
        std::env::set_var("POLARS_FMT_TABLE_FORMATTING", "NOTHING");
        std::env::set_var("POLARS_FMT_MAX_ROWS", "-1");
        std::env::set_var("POLARS_FMT_MAX_COLS", "-1");
        println!("{result:?}");
    } else {
        println!("{result:?}");
    }

    Ok(())
}

fn get_parquet(path: &PathBuf) -> PolarsResult<LazyFrame> {
    let result = LazyFrame::scan_parquet(path, ScanArgsParquet::default());
    result.map_err(|e: PolarsError| e.wrap_msg(&|msg| format!("Couldn't read {path:?} ({msg})")))
}
