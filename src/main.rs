use clap::Parser;
use oracle_logical_replicator::OracleLogicalReplicator;
use std::io::Write;
use log::{error, info};


mod oracle_logical_replicator;
#[macro_use]
mod common;
mod ctx; 
mod transactions;
mod metadata;
mod locales;
mod oradefs;
mod builder;
mod parser;
mod replicators;
use common::errors::Result;
use common::OLRErrorCode::*;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct ReplicatorArgs {
    #[arg(short, long, default_value_t = String::from("scripts/Replicator.json") )]
    file: String
}

fn start(args : ReplicatorArgs) -> Result<()> {
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("OS: {}; Arch: {}; Family: {}", std::env::consts::OS, std::env::consts::ARCH, std::env::consts::FAMILY);

    if !args.file.ends_with(".json") {
        return olr_err!(WrongFileName, "Wrong config file name: {}", args.file);
    } 

    info!("Config file name: {}", args.file);

    if let Err(err) = std::fs::metadata(&args.file) {
        return olr_err!(GetFileMetadata, "Get metadata from file: {} error: {}", args.file, err);
    }

    let replicator: OracleLogicalReplicator = OracleLogicalReplicator::new(args.file);

    replicator.run()
}

#[cfg(debug_assertions)]
fn init_logger() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                "{} {:>16} {:>40}:{:<4} - {}",
                chrono::Local::now().format("\x1b[92m%Y-%m-%d \x1b[93m%H:%M:%S \x1b[0m"),
                match record.level() {
                    log::Level::Error => "[\x1b[91mERROR\x1b[0m]",
                    log::Level::Warn => "[\x1b[95mWARN\x1b[0m]",
                    log::Level::Info => "[\x1b[93mINFO\x1b[0m]",
                    log::Level::Debug => "[\x1b[94mDEBUG\x1b[0m]",
                    log::Level::Trace => "[\x1b[97mTRACE\x1b[0m]",
                },
                record.file().unwrap_or("UNKNOWN FILE"),
                record.line().unwrap_or(u32::MAX),
                record.args(),
            )
        })
        .filter(None, log::LevelFilter::Trace)
        .init();
}

#[cfg(not(debug_assertions))]
fn init_logger() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                "{} {:>16} - {}",
                chrono::Local::now().format("\x1b[92m%Y-%m-%d \x1b[93m%H:%M:%S \x1b[0m"),
                match record.level() {
                    log::Level::Error => "[\x1b[91mERROR\x1b[0m]",
                    log::Level::Warn => "[\x1b[95mWARN\x1b[0m]",
                    log::Level::Info => "[\x1b[93mINFO\x1b[0m]",
                    log::Level::Debug => "[\x1b[94mDEBUG\x1b[0m]",
                    log::Level::Trace => "[\x1b[97mTRACE\x1b[0m]",
                },
                record.args(),
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();
}

fn main() {
    init_logger();

    let args: ReplicatorArgs = ReplicatorArgs::parse();

    let res: Result<()> = start(args);

    match res {
        Ok(_) => {
            info!("Replication stopped");
        }
        Err(err) => {
            error!("{}", err);
            info!("Replication stopped due to error");
        }
    }
}
