use clap::Parser;
use std::{io::Write, process::exit};
use log::{error, info};


mod oracle_logical_replicator;
#[macro_use]
mod common;
mod ctx; 
mod metadata;
mod locales;
mod oradefs;
mod builder;
mod replicators;
use common::errors::OLRError;

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
        return olr_err!(000001, "Wrong config file name: {}", args.file).into();
    } 

    info!("Config file name: {}", args.file);

    if !std::fs::metadata(&args.file).is_ok() {
        return olr_err!(000001, "File does not exist: {}", args.file).into();
    }

    let replicator = oracle_logical_replicator::OracleLogicalReplicator::new(args.file);

    replicator.run()
}

fn main() {
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
                record.file().unwrap(),
                record.line().unwrap(),
                record.args(),
            )
        })
        .filter(None, log::LevelFilter::Trace)
        .init();

    let args = ReplicatorArgs::parse();

    let res = start(args);

    if let Err(err) = res {
        error!("{}", err);
        error!("Replication stopped due to error");
    } else {
        info!("Replication stopped");
    }
}
