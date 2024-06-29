// #![feature(unix_sigpipe)]

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

    /// Output as CSV
    #[arg(long)]
    csv: bool,
}

type MainResult = Result<(), Box<dyn Error>>;
type LazyFrames = Result<Vec<LazyFrame>, PolarsError>;

#[cfg(unix)]
fn reset_sigpipe() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

#[cfg(not(unix))]
fn reset_sigpipe() {
    // no-op
}

// #[unix_sigpipe = "sig_dfl"]
fn main() -> MainResult {
    reset_sigpipe();

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
    let mut result = lf.collect()?;


    if args.csv {
        // let mut file = std::fs::File::create("path.csv").unwrap();
        // CsvWriter::new(&mut file).finish(&mut result).unwrap();
        CsvWriter::new(std::io::stdout().lock()).finish(&mut result).unwrap();
    } else if args.full || !std::io::stdout().is_terminal() {
        CsvWriter::new(std::io::stdout().lock())
            .with_separator(b'\t')
            .include_header(!args.no_header)
            .finish(&mut result).unwrap();
    } else {
        if args.no_header {
            unsafe {
                std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_NAMES", "1");
                std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
            }
        }
        println!("{result:?}");
    }

    Ok(())
}

fn get_parquet(path: &PathBuf) -> PolarsResult<LazyFrame> {
    let result = LazyFrame::scan_parquet(path, ScanArgsParquet::default());
    result.map_err(|e: PolarsError| e.wrap_msg(&|msg| format!("Couldn't read {path:?} ({msg})")))
}
