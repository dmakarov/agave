use clap::{App, Arg};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{atomic::{AtomicBool, Ordering}, Arc},
    thread,
    time::Duration,
};

pub mod rpc_process;
pub mod rpc_service;

fn main() {
    env_logger::init();
    let matches = App::new("solana-json-rpc")
        .version("0.1.0")
        .author("Agave Team <hello@anza.xyz>")
        .about("Solana JSON-RPC server")
        .arg(
            Arg::with_name("text")
                .value_name("TEXT")
                .help("Input text")
                .required(false)
                .min_values(1),
        )
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .default_value("test-ledger")
                .help("Use DIR as ledger location"),
        )
        .get_matches();
    println!("{:#?}", matches);

    let rpc_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let rpc_port = 8899u16;
    let rpc_addr = SocketAddr::new(rpc_addr, rpc_port);

    let config = rpc_process::JsonRpcConfig {
        rpc_threads: 1,
        rpc_niceness_adj: 0,
        max_request_body_size: Some(8192),
    };

    let exit = Arc::new(AtomicBool::new(false));
    let validator_exit = rpc_process::create_exit(exit.clone());

    let _rpc_service = rpc_service::JsonRpcService::new(rpc_addr, config, validator_exit, exit.clone());

    let refresh_interval = Duration::from_millis(250);
    for _i in 0.. {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        thread::sleep(refresh_interval);
    }
    println!("Finished.");
}
