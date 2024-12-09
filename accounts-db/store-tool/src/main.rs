use {
    ahash::HashSet,
    clap::{
        crate_description, crate_name, value_t_or_exit, values_t_or_exit, App, AppSettings, Arg,
        ArgMatches, SubCommand,
    },
    rayon::prelude::*,
    solana_accounts_db::append_vec::AppendVec,
    solana_sdk::{
        account::ReadableAccount, pubkey::Pubkey, system_instruction::MAX_PERMITTED_DATA_LENGTH,
    },
    std::{
        collections::HashMap,
        fs, io,
        mem::ManuallyDrop,
        num::Saturating,
        path::{Path, PathBuf},
    },
};

const CMD_ANALYZE: &str = "analyze";
const CMD_INSPECT: &str = "inspect";
const CMD_SEARCH: &str = "search";

fn main() {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .global_setting(AppSettings::ArgRequiredElseHelp)
        .global_setting(AppSettings::ColoredHelp)
        .global_setting(AppSettings::InferSubcommands)
        .global_setting(AppSettings::UnifiedHelpMessage)
        .global_setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name(CMD_ANALYZE)
                .about("Analyze account storage directory")
                .arg(
                    Arg::with_name("path")
                        .index(1)
                        .takes_value(true)
                        .value_name("PATH")
                        .help("Account storage directory to inspect"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .takes_value(false)
                        .help("Show additional account information"),
                ),
        )
        .subcommand(
            SubCommand::with_name(CMD_INSPECT)
                .about("Inspects an account storage file and display each account's information")
                .arg(
                    Arg::with_name("path")
                        .index(1)
                        .takes_value(true)
                        .value_name("PATH")
                        .help("Account storage file to inspect"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .takes_value(false)
                        .help("Show additional account information"),
                ),
        )
        .subcommand(
            SubCommand::with_name(CMD_SEARCH)
                .about("Searches for accounts")
                .arg(
                    Arg::with_name("path")
                        .index(1)
                        .takes_value(true)
                        .value_name("PATH")
                        .help("Account storage directory to search"),
                )
                .arg(
                    Arg::with_name("addresses")
                        .index(2)
                        .takes_value(true)
                        .value_name("PUBKEYS")
                        .value_delimiter(",")
                        .help("Search for the entries of one or more pubkeys, delimited by commas"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .takes_value(false)
                        .help("Show additional account information"),
                ),
        )
        .get_matches();

    let subcommand = matches.subcommand();
    let subcommand_str = subcommand.0.to_string();
    match subcommand {
        (CMD_ANALYZE, Some(subcommand_matches)) => cmd_analyze(&matches, subcommand_matches),
        (CMD_INSPECT, Some(subcommand_matches)) => cmd_inspect(&matches, subcommand_matches),
        (CMD_SEARCH, Some(subcommand_matches)) => cmd_search(&matches, subcommand_matches),
        _ => unreachable!(),
    }
    .unwrap_or_else(|err| {
        eprintln!("Error: '{subcommand_str}' failed: {err}");
        std::process::exit(1);
    });
}

fn cmd_analyze(
    _app_matches: &ArgMatches<'_>,
    subcommand_matches: &ArgMatches<'_>,
) -> Result<(), String> {
    let path = value_t_or_exit!(subcommand_matches, "path", String);
    let verbose = subcommand_matches.is_present("verbose");
    do_analyze(path, verbose)
}

fn cmd_inspect(
    _app_matches: &ArgMatches<'_>,
    subcommand_matches: &ArgMatches<'_>,
) -> Result<(), String> {
    let path = value_t_or_exit!(subcommand_matches, "path", String);
    let verbose = subcommand_matches.is_present("verbose");
    do_inspect(path, verbose)
}

fn cmd_search(
    _app_matches: &ArgMatches<'_>,
    subcommand_matches: &ArgMatches<'_>,
) -> Result<(), String> {
    let path = value_t_or_exit!(subcommand_matches, "path", String);
    let addresses = values_t_or_exit!(subcommand_matches, "addresses", Pubkey);
    let addresses = HashSet::from_iter(addresses);
    let verbose = subcommand_matches.is_present("verbose");
    do_search(path, addresses, verbose)
}

fn get_files_in(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let path = entry?.path();
        if path.is_file() {
            let path = fs::canonicalize(path)?;
            files.push(path);
        }
    }
    Ok(files)
}

fn do_analyze(
    dir: impl AsRef<Path>,
    _verbose: bool,
) -> Result<(), String> {
    let files = get_files_in(&dir).map_err(|err| {
        format!(
            "failed to get files in dir '{}': {err}",
            dir.as_ref().display(),
        )
    })?;
    let mut accounts: HashMap<Pubkey, Vec<u64>> = HashMap::new();
    let mut storages: HashMap<u64, HashMap<Pubkey, usize>> = HashMap::new();
    let mut acc_in_more_than_two_storages = 0u64;
    files.iter().for_each(|file| {
        let Ok(storage) = AppendVec::new_for_store_tool(file).inspect_err(|err| {
            eprintln!(
                "failed to open account storage file '{}': {err}",
                file.display(),
            )
        }) else {
            return;
        };
        // By default, when the AppendVec is dropped, the backing file will be removed.
        // We do not want to remove the backing file here in the store-tool, so prevent dropping.
        let storage = ManuallyDrop::new(storage);
        let slot = file.file_stem().expect("path is a file");
        let slot = slot.to_str().unwrap();
        let slot = u64::from_str_radix(slot, 10).expect("unexpected file name");
        let mut storage_entry = HashMap::new();
        storage.scan_accounts(|account| {
            let pubkey = account.pubkey();
            let size = account.data_len();
            storage_entry.insert(*pubkey, size);
            if let Some(entry) = accounts.get_mut(pubkey) {
                entry.push(slot);
            } else {
                let mut v = Vec::new();
                v.push(slot);
                accounts.insert(*pubkey, v);
            }
        });
        if storages.contains_key(&slot) {
            println!("two different storages for the same slot {slot}");
        }
        storages.insert(slot, storage_entry);
    });

    let mut dead_bytes: usize = 0;
    let dead_count = accounts.iter().filter(|(k, v)| {
        if v.len() > 1 {
            if v.len() > 2 {
                acc_in_more_than_two_storages += 1;
            }
            let mut v_sorted = Vec::with_capacity(v.len());
            (0..v.len()).for_each(|_| v_sorted.push(0));
            v_sorted.copy_from_slice(v);
            v_sorted.sort();
            v_sorted.pop();
            dead_bytes += v_sorted.iter().map(|v| storages.get(v).unwrap().get(k).unwrap()).sum::<usize>();
            true
        } else {
            false
        }
    }).count();
    println!("found {} unique pubkeys, of which {} are dead, total dead_bytes {}", accounts.len(), dead_count, dead_bytes);
    println!("{} accounts found in more than 2 storages", acc_in_more_than_two_storages);

    Ok(())
}

fn do_inspect(file: impl AsRef<Path>, verbose: bool) -> Result<(), String> {
    let storage = AppendVec::new_for_store_tool(file.as_ref()).map_err(|err| {
        format!(
            "failed to open account storage file '{}': {err}",
            file.as_ref().display(),
        )
    })?;
    // By default, when the AppendVec is dropped, the backing file will be removed.
    // We do not want to remove the backing file here in the store-tool, so prevent dropping.
    let storage = ManuallyDrop::new(storage);

    let data_size_width = width10(MAX_PERMITTED_DATA_LENGTH);
    let offset_width = width16(storage.capacity());

    let mut num_accounts = Saturating(0usize);
    let mut stored_accounts_size = Saturating(0);
    let mut lamports = Saturating(0);
    storage.scan_accounts(|account| {
        if verbose {
            println!("{account:?}");
        } else {
            println!(
                "{:#0offset_width$x}: {:44}, owner: {:44}, data size: {:data_size_width$}, lamports: {}",
                account.offset(),
                account.pubkey().to_string(),
                account.owner().to_string(),
                account.data_len(),
                account.lamports(),
            );
        }
        num_accounts += 1;
        stored_accounts_size += account.stored_size();
        lamports += account.lamports();
    });

    println!(
        "number of accounts: {}, stored accounts size: {}, file size: {}, lamports: {}",
        num_accounts,
        stored_accounts_size,
        storage.capacity(),
        lamports,
    );
    Ok(())
}

fn do_search(
    dir: impl AsRef<Path>,
    addresses: HashSet<Pubkey>,
    verbose: bool,
) -> Result<(), String> {
    let files = get_files_in(&dir).map_err(|err| {
        format!(
            "failed to get files in dir '{}': {err}",
            dir.as_ref().display(),
        )
    })?;
    files.par_iter().for_each(|file| {
        let Ok(storage) = AppendVec::new_for_store_tool(file).inspect_err(|err| {
            eprintln!(
                "failed to open account storage file '{}': {err}",
                file.display(),
            )
        }) else {
            return;
        };
        // By default, when the AppendVec is dropped, the backing file will be removed.
        // We do not want to remove the backing file here in the store-tool, so prevent dropping.
        let storage = ManuallyDrop::new(storage);

        let file_name = Path::new(file.file_name().expect("path is a file"));
        storage.scan_accounts(|account| {
            if addresses.contains(account.pubkey()) {
                if verbose {
                    println!("storage: {}, {account:?}", file_name.display());
                } else {
                    println!(
                        "storage: {}, offset: {}, pubkey: {}, owner: {}, data size: {}, lamports: {}",
                        file_name.display(),
                        account.offset(),
                        account.pubkey(),
                        account.owner(),
                        account.data_len(),
                        account.lamports(),
                    );
                }
            }
        });
    });

    Ok(())
}

/// Returns the number of characters required to print `x` in base-10
fn width10(x: u64) -> usize {
    (x as f64).log10().ceil() as usize
}

/// Returns the number of characters required to print `x` in base-16
fn width16(x: u64) -> usize {
    (x as f64).log(16.0).ceil() as usize
}
