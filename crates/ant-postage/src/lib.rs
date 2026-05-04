//! Postage stamp issuance — matches bee `pkg/postage/stamp.go` + `stampissuer.go`
//! serialization and signing (`signer.Sign` ⇒ EIP‑191 wrapping of digest).

use ant_crypto::{
    ethereum_address_from_public_key, recover_public_key, sign_handshake_data, CryptoError,
    SECP256K1_SECRET_LEN,
};
use k256::ecdsa::VerifyingKey;
use sha3::{Digest, Keccak256};
use thiserror::Error;

pub const INDEX_SIZE: usize = 8;
pub const TIMESTAMP_SIZE: usize = 8;
pub const SIGNATURE_LEN: usize = 65;
pub const STAMP_SIZE: usize = 32 + INDEX_SIZE + TIMESTAMP_SIZE + SIGNATURE_LEN;

#[derive(Debug, Error)]
pub enum PostageError {
    #[error("crypto: {0}")]
    Crypto(#[from] CryptoError),
    #[error("{0}")]
    Msg(String),
    #[error("bucket full")]
    BucketFull,
}

/// Bee `stampIssuerData`-shaped issuer with only the fields Ant needs locally.
#[derive(Debug, Clone)]
pub struct StampIssuer {
    batch_id: [u8; 32],
    batch_depth: u8,
    bucket_depth: u8,
    immutable: bool,
    buckets: Vec<u32>,
}

impl StampIssuer {
    pub fn new(
        batch_id: [u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        immutable: bool,
    ) -> Result<Self, PostageError> {
        if bucket_depth == 0 || bucket_depth >= batch_depth {
            return Err(PostageError::Msg(
                "bucketDepth must be >0 and < batchDepth".into(),
            ));
        }
        let n = 1usize
            .checked_shl(u32::from(bucket_depth))
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| PostageError::Msg("bucket bitmap too large".into()))?;
        Ok(Self {
            batch_id,
            batch_depth,
            bucket_depth,
            immutable,
            buckets: vec![0u32; n],
        })
    }

    pub fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    pub fn batch_depth(&self) -> u8 {
        self.batch_depth
    }

    pub fn batch_id(&self) -> &[u8; 32] {
        &self.batch_id
    }

    fn bucket_upper_bound(&self) -> u32 {
        1u32 << u32::from(self.batch_depth - self.bucket_depth)
    }

    pub fn increment(
        &mut self,
        addr: &[u8; 32],
    ) -> Result<([u8; INDEX_SIZE], [u8; TIMESTAMP_SIZE]), PostageError> {
        let b_idx = collision_bucket_from_addr(self.bucket_depth, addr);
        let cnt = self.buckets.get(b_idx as usize).copied().unwrap_or(0);
        let upper = self.bucket_upper_bound();

        let mut cnt = cnt;
        if cnt == upper {
            if self.immutable {
                return Err(PostageError::BucketFull);
            }
            cnt = 0;
            self.buckets[b_idx as usize] = 0;
        }

        cnt += 1;
        self.buckets[b_idx as usize] = cnt;

        Ok((index_bytes(b_idx, cnt - 1), unix_now_nanos_be()))
    }
}

pub fn unix_now_nanos_be() -> [u8; TIMESTAMP_SIZE] {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut b = [0u8; TIMESTAMP_SIZE];
    b.copy_from_slice(&now.to_be_bytes());
    b
}

/// Matches bee `pkg/postage/stampissuer.go::toBucket`: top `depth` bits of address as BE u32,
/// masked to collision bucket slots.
pub fn collision_bucket_from_addr(bucket_depth: u8, addr: &[u8; 32]) -> u32 {
    let top = u32::from_be_bytes(addr[0..4].try_into().unwrap());
    top >> (32 - u32::from(bucket_depth))
}

fn index_bytes(bucket: u32, within_bucket: u32) -> [u8; INDEX_SIZE] {
    let mut out = [0u8; INDEX_SIZE];
    out[0..4].copy_from_slice(&bucket.to_be_bytes());
    out[4..8].copy_from_slice(&within_bucket.to_be_bytes());
    out
}

pub fn postage_sign_digest(chunk_addr: &[u8; 32], batch_id: &[u8; 32], index: &[u8; 8], ts: &[u8; 8]) -> [u8; 32] {
    let mut h = Keccak256::default();
    h.update(chunk_addr);
    h.update(batch_id);
    h.update(index);
    h.update(ts);
    let out = h.finalize();
    out.into()
}

/// Build a stamped full binary stamp — same layout as bee `Stamp.MarshalBinary`.
pub fn sign_stamp_bytes(
    secret: &[u8; SECP256K1_SECRET_LEN],
    issuer: &mut StampIssuer,
    chunk_address: &[u8; 32],
) -> Result<[u8; STAMP_SIZE], PostageError> {
    let (index, ts) = issuer.increment(chunk_address)?;
    let digest = postage_sign_digest(chunk_address, &issuer.batch_id, &index, &ts);
    let sig = sign_handshake_data(secret, &digest)?;

    let mut stamp = [0u8; STAMP_SIZE];
    stamp[0..32].copy_from_slice(&issuer.batch_id);
    stamp[32..40].copy_from_slice(&index);
    stamp[40..48].copy_from_slice(&ts);
    stamp[48..113].copy_from_slice(&sig);
    Ok(stamp)
}

pub fn stamp_batch_id(stamp: &[u8]) -> Result<&[u8; 32], PostageError> {
    if stamp.len() != STAMP_SIZE {
        return Err(PostageError::Msg("bad stamp size".into()));
    }
    Ok(stamp[0..32].try_into().unwrap())
}

/// Verify ECDSA signer is `expected_eth` — matches bee `RecoverBatchOwner`.
pub fn verify_stamp_owner(
    chunk_addr: &[u8; 32],
    stamp: &[u8; STAMP_SIZE],
    expected_eth: &[u8; 20],
    depth: u8,
    bucket_depth: u8,
) -> Result<(), PostageError> {
    let batch_id = stamp[0..32].try_into().unwrap();
    let index: &[u8; 8] = stamp[32..40].try_into().unwrap();
    let ts: &[u8; 8] = stamp[40..48].try_into().unwrap();
    let sig: &[u8; 65] = stamp[48..113].try_into().unwrap();

    let digest_arr = postage_sign_digest(chunk_addr, batch_id, index, ts);
    let digest_slice: &[u8] = digest_arr.as_ref();
    let vk = recover_public_key(sig, digest_slice)?;

    bucket_check(chunk_addr, index, bucket_depth, depth)?;

    let eth = ethereum_address_from_public_key(&vk);
    if eth != *expected_eth {
        return Err(PostageError::Msg("stamp owner mismatch".into()));
    }
    Ok(())
}

fn bucket_check(
    chunk_addr: &[u8; 32],
    index: &[u8; 8],
    bucket_depth: u8,
    depth: u8,
) -> Result<(), PostageError> {
    let bucket = collision_bucket_from_addr(bucket_depth, chunk_addr);
    let exp_bucket = read_u32_be(&index[0..4]);
    if bucket != exp_bucket {
        return Err(PostageError::Msg("bucket mismatch".into()));
    }

    let within = read_u32_be(&index[4..8]);
    let max_idx = 1u32 << u32::from(depth - bucket_depth);
    if within >= max_idx {
        return Err(PostageError::Msg("invalid stamp index".into()));
    }
    Ok(())
}

fn read_u32_be(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}

/// Optional signer identity check helper.
pub fn eth_address_matches_key(pubkey_eth: &[u8; 20], vk: &VerifyingKey) -> bool {
    ethereum_address_from_public_key(vk) == *pubkey_eth
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::random_secp256k1_secret;

    #[test]
    fn stamp_roundtrip_owner() {
        let secret = random_secp256k1_secret();
        let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into()).unwrap();
        let vk = *sk.verifying_key();
        let owner = ethereum_address_from_public_key(&vk);
        let mut batch = [0u8; 32];
        batch[0] = 0xab;
        let mut issuer = StampIssuer::new(batch, 21, 16, false).unwrap();
        let chunk = [7u8; 32];

        let stamp = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        verify_stamp_owner(&chunk, &stamp, &owner, issuer.batch_depth, issuer.bucket_depth).unwrap();
    }
}
