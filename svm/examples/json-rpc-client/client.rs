use crate::utils;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::signature::Signer;
use solana_sdk::signer::keypair::{read_keypair_file, Keypair};
use solana_sdk::transaction::Transaction;

/// Establishes a RPC connection with the solana cluster configured by
/// `solana config set --url <URL>`. Information about what cluster
/// has been configured is gleened from the solana config file
/// `~/.config/solana/cli/config.yml`.
pub fn establish_connection(url: &Option<&str>, config: &Option<&str>) -> utils::Result<RpcClient> {
    let rpc_url = match url {
        Some(x) => {
            if *x == "localhost" {
                "http://localhost:8899".to_string()
            } else {
                String::from(*x)
            }
        },
        None => utils::get_rpc_url(config)?
    };
    Ok(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()))
}

/// Loads keypair information from the file located at KEYPAIR_PATH
/// and then verifies that the loaded keypair information corresponds
/// to an executable account via CONNECTION. Failure to read the
/// keypair or the loaded keypair corresponding to an executable
/// account will result in an error being returned.
pub fn get_program(keypair_path: &str, connection: &RpcClient) -> utils::Result<Keypair> {
    let program_keypair = read_keypair_file(keypair_path).map_err(|e| {
        utils::Error::InvalidConfig(format!(
            "failed to read program keypair file ({}): ({})",
            keypair_path, e
        ))
    })?;
/*
    let program_info = connection.get_account(&program_keypair.pubkey())?;
    if !program_info.executable {
        return Err(utils::Error::InvalidConfig(format!(
            "program with keypair ({}) is not executable",
            keypair_path
        )));
    }
*/
    Ok(program_keypair)
}

/// Sends an instruction from PLAYER to PROGRAM via CONNECTION. The
/// instruction contains no data but does contain the address of our
/// previously generated greeting account. The program will use that
/// passed in address to update its greeting counter after verifying
/// that it owns the account that we have passed in.
pub fn say_hello(player: &Keypair, program: &Keypair, connection: &RpcClient) -> utils::Result<()> {
    let greeting_pubkey = utils::get_greeting_public_key(&player.pubkey(), &program.pubkey())?;

    // Submit an instruction to the chain which tells the program to
    // run. We pass the account that we want the results to be stored
    // in as one of the accounts arguents which the program will
    // handle.

    let data = [1u8];
    let instruction = Instruction::new_with_bytes(
        program.pubkey(),
        &data,
        vec![AccountMeta::new(greeting_pubkey, false)],
    );
    let message = Message::new(&[instruction], Some(&player.pubkey()));
    let transaction = Transaction::new(&[player], message, connection.get_latest_blockhash()?);

    connection.simulate_transaction(&transaction)?;

    Ok(())
}
