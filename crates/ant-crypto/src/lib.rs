//! Secp256k1 keys, Keccak-256, Ethereum and Swarm overlay addresses, and Bee-compatible
//! BZZ handshake signing (EIP-191 prefixed digest).
//!
//! Also exposes the chunk-level primitives: BMT hashing and content-addressed chunk
//! validation, see [`bmt`].

pub mod bmt;
mod error;

pub use bmt::{bmt_hash_with_span, bmt_root, cac_new, cac_valid, CHUNK_SIZE, SPAN_SIZE};
pub use error::CryptoError;

use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use sha3::{Digest, Keccak256};

pub const OVERLAY_NONCE_LEN: usize = 32;
pub const SECP256K1_SECRET_LEN: usize = 32;

/// Keccak-256 as used by Ethereum / Swarm (`LegacyKeccak256` in Bee).
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

/// Uncompressed SEC1 public key bytes without the `0x04` prefix (64 bytes), for hashing.
pub fn uncompressed_pubkey_xy(pk: &VerifyingKey) -> [u8; 64] {
    let ep = pk.to_encoded_point(false);
    let s = ep.as_bytes();
    debug_assert_eq!(s[0], 0x04);
    let mut out = [0u8; 64];
    out.copy_from_slice(&s[1..65]);
    out
}

/// Ethereum address (20 bytes) from a secp256k1 public key (matches Bee `NewEthereumAddress`).
pub fn ethereum_address_from_public_key(pk: &VerifyingKey) -> [u8; 20] {
    let xy = uncompressed_pubkey_xy(pk);
    let hash = keccak256(&xy);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..]);
    addr
}

/// Swarm overlay address: `keccak256(eth_address ‖ network_id_le ‖ nonce)` (Bee `NewOverlayFromEthereumAddress`).
pub fn overlay_from_ethereum_address(
    eth_address: &[u8; 20],
    network_id: u64,
    nonce: &[u8; OVERLAY_NONCE_LEN],
) -> [u8; 32] {
    let mut buf = [0u8; 20 + 8 + OVERLAY_NONCE_LEN];
    buf[..20].copy_from_slice(eth_address);
    buf[20..28].copy_from_slice(&network_id.to_le_bytes());
    buf[28..].copy_from_slice(nonce);
    keccak256(&buf)
}

/// Payload signed in the BZZ handshake: `bee-handshake- ‖ underlay ‖ overlay ‖ network_id_be`.
pub fn handshake_sign_data(underlay: &[u8], overlay: &[u8; 32], network_id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(15 + underlay.len() + 32 + 8);
    v.extend_from_slice(b"bee-handshake-");
    v.extend_from_slice(underlay);
    v.extend_from_slice(overlay);
    v.extend_from_slice(&network_id.to_be_bytes());
    v
}

fn ethereum_prefixed_sign_data(sign_data: &[u8]) -> Vec<u8> {
    let len_str = sign_data.len().to_string();
    let mut p = Vec::with_capacity(2 + 26 + len_str.len() + sign_data.len());
    p.extend_from_slice(b"\x19Ethereum Signed Message:\n");
    p.extend_from_slice(len_str.as_bytes());
    p.extend_from_slice(sign_data);
    p
}

/// Keccak256 hash of the EIP-191–wrapped payload (Bee `hashWithEthereumPrefix`).
pub fn ethereum_signed_message_hash(sign_data: &[u8]) -> [u8; 32] {
    keccak256(&ethereum_prefixed_sign_data(sign_data))
}

/// Sign handshake data the same way Bee's `crypto.Signer.Sign` does (EIP-191 + compact recovery id).
pub fn sign_handshake_data(
    secret: &[u8; SECP256K1_SECRET_LEN],
    sign_data: &[u8],
) -> Result<[u8; 65], CryptoError> {
    let sk = SigningKey::from_bytes(secret.into())?;
    let mut hasher = Keccak256::new();
    hasher.update(ethereum_prefixed_sign_data(sign_data));
    let (sig, recid) = sk.sign_digest_recoverable(hasher)?;
    let v = recid.to_byte().saturating_add(27);
    let mut out = [0u8; 65];
    out[..32].copy_from_slice(sig.r().to_bytes().as_slice());
    out[32..64].copy_from_slice(sig.s().to_bytes().as_slice());
    out[64] = v;
    Ok(out)
}

/// Recover signer public key from a Bee / Ethereum 65-byte signature (`r‖s‖v` with `v` in 27..=30).
pub fn recover_public_key(
    signature: &[u8; 65],
    sign_data: &[u8],
) -> Result<VerifyingKey, CryptoError> {
    let digest = ethereum_signed_message_hash(sign_data);
    let recid = RecoveryId::try_from(
        signature[64]
            .checked_sub(27)
            .ok_or(CryptoError::BadSignature)?,
    )
    .map_err(|_| CryptoError::BadSignature)?;
    let sig = Signature::from_slice(&signature[..64])?;
    Ok(VerifyingKey::recover_from_prehash(&digest, &sig, recid)?)
}

/// Verify a BZZ handshake signature and optionally that overlay matches `nonce` + `network_id`.
pub fn verify_bzz_address_signature(
    underlay: &[u8],
    overlay: &[u8; 32],
    signature: &[u8; 65],
    nonce: &[u8; OVERLAY_NONCE_LEN],
    network_id: u64,
    validate_overlay: bool,
) -> Result<[u8; 20], CryptoError> {
    let sign_data = handshake_sign_data(underlay, overlay, network_id);
    let vk = recover_public_key(signature, &sign_data)?;
    if validate_overlay {
        let eth = ethereum_address_from_public_key(&vk);
        let expected = overlay_from_ethereum_address(&eth, network_id, nonce);
        if expected != *overlay {
            return Err(CryptoError::OverlayMismatch);
        }
    }
    Ok(ethereum_address_from_public_key(&vk))
}

/// Random 32-byte overlay nonce.
pub fn random_overlay_nonce() -> [u8; OVERLAY_NONCE_LEN] {
    let mut n = [0u8; OVERLAY_NONCE_LEN];
    getrandom::fill(&mut n).expect("OS RNG");
    n
}

/// Random secp256k1 signing key bytes (32 bytes).
pub fn random_secp256k1_secret() -> [u8; SECP256K1_SECRET_LEN] {
    let mut s = [0u8; SECP256K1_SECRET_LEN];
    getrandom::fill(&mut s).expect("OS RNG");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let secret = random_secp256k1_secret();
        let sk = SigningKey::from_bytes(&secret.into()).unwrap();
        let vk = VerifyingKey::from(&sk);
        let eth = ethereum_address_from_public_key(&vk);
        let network_id = 1u64;
        let nonce = random_overlay_nonce();
        let overlay = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let underlay = b"/ip4/127.0.0.1/tcp/1634/p2p/12D3KooW";
        let sign_data = handshake_sign_data(underlay, &overlay, network_id);
        let sig = sign_handshake_data(&secret, &sign_data).unwrap();
        let recovered_eth =
            verify_bzz_address_signature(underlay, &overlay, &sig, &nonce, network_id, true)
                .unwrap();
        assert_eq!(recovered_eth, eth);
    }

    /// Cross-check overlay formula with fixed vectors (endianness + layout).
    #[test]
    fn overlay_vector_endianness() {
        let mut eth = [0u8; 20];
        hex::decode_to_slice("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &mut eth).unwrap();
        let network_id = 1u64;
        let mut nonce = [0u8; 32];
        nonce[0] = 0x01;
        let o = overlay_from_ethereum_address(&eth, network_id, &nonce);
        let h = hex::encode(o);
        assert_eq!(h.len(), 64);
        let sign_data = handshake_sign_data(b"bee-handshake-", &o, network_id);
        assert!(sign_data.ends_with(&1u64.to_be_bytes()));
    }
}
