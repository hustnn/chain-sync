use ethers::{prelude::*};
use ethers::utils::hex;

pub fn h64_to_hex(v: &H64) -> String {
    "0x".to_owned() + &hex::encode(v)
}

pub fn h160_to_hex(v: &H160) -> String {
    "0x".to_owned() + &hex::encode(v)
}

pub fn h256_to_hex(v: &H256) -> String {
    "0x".to_owned() + &hex::encode(v)
}

pub fn u256_to_hex(v: &U256) -> String {
    format!("{:#}", v)
}

pub fn bloom_to_hex(v: &Bloom) -> String {
    "0x".to_owned() + &hex::encode(v.0)
}

// Bytes to hex with 0x prefix.
pub fn bytes_to_hex(v: &Bytes) -> String {
    "0x".to_string() + &hex::encode(v)
}