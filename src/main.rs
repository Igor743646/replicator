use clap::Parser;
use std::io::Write;
use log::{error, info};

mod errors;
use errors::OracleDBReplicatorError as ODBRError;
mod replicator;
mod ctx;
mod constants;
mod types;
mod metadata;
mod locales;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct ReplicatorArgs {
    #[arg(short, long, default_value_t = String::from("scripts/Replicator.json") )]
    file: String
}

fn start(args : ReplicatorArgs) -> Result<(), ODBRError> {
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("OS: {}; Arch: {}; Family: {}", std::env::consts::OS, std::env::consts::ARCH, std::env::consts::FAMILY);

    if !args.file.ends_with(".json") {
        return ODBRError::new(000001, format!("Wrong config file name: {0}", args.file)).err();
    } 

    info!("Config file name: {0}", args.file);

    if !std::fs::metadata(&args.file).is_ok() {
        return ODBRError::new(000001, format!("File does not exist: {0}", args.file)).err();
    }

    let replicator = replicator::OracleDBReplicator::new(args.file);

    replicator.run()
}

fn main() {

    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                "{} [{:5}] {:>30} ({:>4}) - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.file().unwrap(),
                record.line().unwrap(),
                record.args(),
            )
        })
        .filter(None, log::LevelFilter::Debug)
        .init();

    let args = ReplicatorArgs::parse();

    let res = start(args);

    if let Err(err) = res {
        error!("{}", err);
    }
}
