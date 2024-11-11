use clap::Parser;
use std::io::Write;
use std::u32;
use log::{error, info};


mod oracle_logical_replicator;
#[macro_use]
mod common;
mod ctx; 
mod metadata;
mod locales;
mod oradefs;
mod builder;
mod parser;
mod replicators;
use common::errors::OLRError;
use common::OLRErrorCode::*;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct ReplicatorArgs {
    #[arg(short, long, default_value_t = String::from("scripts/Replicator.json") )]
    file: String
}

fn start(args : ReplicatorArgs) -> Result<(), OLRError> {
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("OS: {}; Arch: {}; Family: {}", std::env::consts::OS, std::env::consts::ARCH, std::env::consts::FAMILY);

    if !args.file.ends_with(".json") {
        return olr_err!(WrongFileName, "Wrong config file name: {}", args.file).into();
    } 

    info!("Config file name: {}", args.file);

    if let Err(err) = std::fs::metadata(&args.file) {
        return olr_err!(GetFileMetadata, "Get metadata from file: {} error: {}", args.file, err.to_string()).into();
    }

    let replicator = oracle_logical_replicator::OracleLogicalReplicator::new(args.file);

    replicator.run()
}

fn init_logger() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                "{} [{}\u{001b}[0;37;40m] {:>40} ({:>4}) - {}",
                chrono::Local::now().format("\u{001b}[0;32;40m%Y-%m-%d \u{001b}[0;33;40m%H:%M:%S \u{001b}[0;37;40m"),
                match record.level() {
                    log::Level::Error => "\u{001b}[38;5;124mERROR",
                    log::Level::Warn => "\u{001b}[38;5;196m WARN",
                    log::Level::Info => "\u{001b}[38;5;226m INFO",
                    log::Level::Debug => "\u{001b}[38;5;020mDEBUG",
                    log::Level::Trace => "\u{001b}[38;5;15mTRACE",
                },
                record.file().unwrap_or("UNKNOWN FILE"),
                record.line().unwrap_or(u32::MAX),
                record.args(),
            )
        })
        .filter(None, log::LevelFilter::Trace)
        .init();
}

fn main() {
    init_logger();

    let args = ReplicatorArgs::parse();

    let res = start(args);

    if let Err(err) = res {
        error!("{}", err);
        error!("Replication stopped due to error");
    } else {
        info!("Replication stopped");
    }
}
