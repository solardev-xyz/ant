//! Mine an overlay nonce so a node's overlay lands within `min_bits`
//! proximity of a target neighborhood — the primitive for placing two
//! light nodes co-resident in one rendezvous-room neighborhood (so both
//! reliably receive), used by the symmetric many-to-many PSS demo.
//!
//! Usage:
//!   cargo run -p ant-crypto --example mine_overlay -- \
//!     <eth_hex(40)> <network_id> <target_overlay_hex(64)> <min_bits>
//!
//! Prints the winning `overlay_nonce` (hex), the resulting overlay, and
//! the proximity (leading matching bits) to the target.

use ant_crypto::{overlay_from_ethereum_address, random_overlay_nonce};

fn leading_match_bits(a: &[u8; 32], b: &[u8; 32]) -> u32 {
    let mut bits = 0u32;
    for i in 0..32 {
        let x = a[i] ^ b[i];
        if x == 0 {
            bits += 8;
        } else {
            bits += x.leading_zeros();
            break;
        }
    }
    bits
}

fn hex_to<const N: usize>(s: &str) -> [u8; N] {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let mut out = [0u8; N];
    hex::decode_to_slice(s, &mut out).expect("hex");
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 5,
        "usage: mine_overlay <eth_hex> <network_id> <target_overlay_hex> <min_bits>"
    );
    let eth: [u8; 20] = hex_to(&args[1]);
    let network_id: u64 = args[2].parse().expect("network_id");
    let target: [u8; 32] = hex_to(&args[3]);
    let min_bits: u32 = args[4].parse().expect("min_bits");

    let mut tries: u64 = 0;
    loop {
        let nonce = random_overlay_nonce();
        let overlay = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let bits = leading_match_bits(&overlay, &target);
        tries += 1;
        if bits >= min_bits {
            println!("nonce={}", hex::encode(nonce));
            println!("overlay={}", hex::encode(overlay));
            println!("proximity_bits={bits}");
            println!("tries={tries}");
            return;
        }
    }
}
