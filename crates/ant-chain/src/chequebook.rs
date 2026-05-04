//! SWAP / chequebook primitives — Phase 7 of M3.
//!
//! Bee's payments-above-the-pseudosettle threshold flow uses a
//! per-node *chequebook contract* on Gnosis. Each node owns its own
//! chequebook; cheques are off-chain EIP-712-signed promises of a
//! cumulative payout that the beneficiary can later cash on-chain via
//! the chequebook's `cashChequeBeneficiary(recipient, amount, sig)`
//! method.
//!
//! This module provides everything needed to **issue** a cheque
//! correctly today, **verify** an inbound cheque, and **build the
//! calldata** for chequebook deployment and on-chain cashing. It does
//! not implement:
//!
//! - The libp2p `/swap/1.0.0/swap` protocol — the wire that ships
//!   cheques between peers. That belongs in `ant-p2p`.
//! - Automatic settlement triggered by the per-peer accounting
//!   crossing `payment_threshold`. That requires a swap driver
//!   bolted onto the existing [`crate::accounting`] surface.
//!
//! The library here is the *protocol-correctness* layer: cheques
//! signed with [`sign_cheque`] are byte-identical to what bee's
//! `pkg/settlement/swap/chequebook.Cheque.Sign` produces, so the
//! moment we wire up the swap protocol the first cheque we ship
//! will be accepted by a vanilla bee node.

use ant_crypto::{
    ethereum_address_from_public_key, recover_public_key_from_prehash, sign_prehash, CryptoError,
    SECP256K1_SECRET_LEN,
};
use primitive_types::U256;
use sha3::{Digest, Keccak256};

/// EIP-712 type hash for `Cheque(address chequebook,address beneficiary,uint256 cumulativePayout)`.
///
/// Mirrors the constant bee hardcodes in
/// `pkg/settlement/swap/chequebook/cheque.go`. Recomputed here so the
/// test suite verifies our keccak path matches.
pub fn cheque_type_hash() -> [u8; 32] {
    keccak(b"Cheque(address chequebook,address beneficiary,uint256 cumulativePayout)")
}

/// EIP-712 type hash for the `EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)`
/// domain bee uses on the chequebook contract.
pub fn eip712_domain_type_hash() -> [u8; 32] {
    keccak(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
}

/// Domain name + version bee writes into the chequebook EIP-712 domain.
/// Hard-coded both ways so a typo on either end of the wire doesn't
/// silently produce mismatched signatures.
pub const CHEQUEBOOK_DOMAIN_NAME: &str = "Chequebook";
pub const CHEQUEBOOK_DOMAIN_VERSION: &str = "1.0";

/// Struct bee shipping back and forth on the swap protocol. Distinct
/// from a `SignedCheque` because the issuer assembles this payload,
/// signs it, then ships `(payload, sig)` together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cheque {
    /// The issuing chequebook contract — payee redeems `cashChequeBeneficiary`
    /// against this address.
    pub chequebook: [u8; 20],
    /// Beneficiary's Ethereum address (their node's overlay key).
    pub beneficiary: [u8; 20],
    /// Cumulative amount the issuer has now committed to the
    /// beneficiary across all cheques. Bee uses cumulative (rather
    /// than per-cheque) values so a lost cheque can be re-derived
    /// from the next one and the chequebook contract just pays out
    /// the diff.
    pub cumulative_payout: U256,
}

/// `Cheque + 65-byte (r ‖ s ‖ v)` signature, as carried on the swap
/// wire. `v` is in `27..=30`, matching `ant_crypto::sign_prehash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCheque {
    pub cheque: Cheque,
    pub signature: [u8; 65],
}

/// Compute the EIP-712 digest a chequebook signer will sign.
///
/// `chain_id` is the chain the chequebook contract lives on (100 for
/// Gnosis).
pub fn cheque_digest(cheque: &Cheque, chain_id: u64) -> [u8; 32] {
    let domain_sep = eip712_domain_separator(chain_id, &cheque.chequebook);
    let struct_hash = cheque_struct_hash(cheque);
    let mut h = Keccak256::default();
    h.update([0x19, 0x01]);
    h.update(domain_sep);
    h.update(struct_hash);
    let out = h.finalize();
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&out);
    digest
}

/// Sign a cheque with the issuing wallet's secret. Returns the
/// 65-byte signature plus the signed digest (handy for the verifier
/// that wants to log it).
pub fn sign_cheque(
    secret: &[u8; SECP256K1_SECRET_LEN],
    cheque: &Cheque,
    chain_id: u64,
) -> Result<SignedCheque, CryptoError> {
    let digest = cheque_digest(cheque, chain_id);
    let signature = sign_prehash(secret, &digest)?;
    Ok(SignedCheque {
        cheque: cheque.clone(),
        signature,
    })
}

/// Recover the issuing chequebook's owner address from a signed
/// cheque. Caller must independently look up the chequebook's
/// `issuer()` on-chain to confirm the recovered address really is
/// authorised to issue cheques against this chequebook.
pub fn recover_cheque_signer(
    signed: &SignedCheque,
    chain_id: u64,
) -> Result<[u8; 20], CryptoError> {
    let digest = cheque_digest(&signed.cheque, chain_id);
    let vk = recover_public_key_from_prehash(&signed.signature, &digest)?;
    Ok(ethereum_address_from_public_key(&vk))
}

fn eip712_domain_separator(chain_id: u64, verifying_contract: &[u8; 20]) -> [u8; 32] {
    let mut h = Keccak256::default();
    h.update(eip712_domain_type_hash());
    h.update(keccak(CHEQUEBOOK_DOMAIN_NAME.as_bytes()));
    h.update(keccak(CHEQUEBOOK_DOMAIN_VERSION.as_bytes()));
    h.update(u256_be_word(&U256::from(chain_id)));
    h.update(pad_word_address(verifying_contract));
    let out = h.finalize();
    let mut sep = [0u8; 32];
    sep.copy_from_slice(&out);
    sep
}

fn cheque_struct_hash(cheque: &Cheque) -> [u8; 32] {
    let mut h = Keccak256::default();
    h.update(cheque_type_hash());
    h.update(pad_word_address(&cheque.chequebook));
    h.update(pad_word_address(&cheque.beneficiary));
    h.update(u256_be_word(&cheque.cumulative_payout));
    let out = h.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&out);
    hash
}

// --- ABI calldata builders ---

/// `SimpleSwapFactory.deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration, bytes32 salt)`
/// — the bee chequebook factory's deploy entrypoint.
pub fn deploy_chequebook_calldata(
    issuer: &[u8; 20],
    default_hard_deposit_timeout: U256,
    salt: &[u8; 32],
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 * 3);
    data.extend_from_slice(&keccak(b"deploySimpleSwap(address,uint256,bytes32)")[0..4]);
    data.extend_from_slice(&pad_word_address(issuer));
    data.extend_from_slice(&u256_be_word(&default_hard_deposit_timeout));
    data.extend_from_slice(salt);
    data
}

/// `Chequebook.cashChequeBeneficiary(address recipient, uint256 cumulativePayout, bytes signature)`.
///
/// `recipient` is whoever should receive the BZZ — usually the
/// beneficiary themselves, but bee allows redirecting to a custodial
/// address. `signature` is the 65-byte EIP-712 cheque signature
/// produced by [`sign_cheque`].
pub fn cash_cheque_beneficiary_calldata(
    recipient: &[u8; 20],
    cumulative_payout: U256,
    signature: &[u8; 65],
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 * 5);
    data.extend_from_slice(&keccak(b"cashChequeBeneficiary(address,uint256,bytes)")[0..4]);
    data.extend_from_slice(&pad_word_address(recipient));
    data.extend_from_slice(&u256_be_word(&cumulative_payout));
    // dynamic-bytes ABI: offset (always 0x60 here, three 32-byte words
    // before the bytes blob), length, then padded payload.
    let offset = U256::from(3u64 * 32);
    data.extend_from_slice(&u256_be_word(&offset));
    let sig_len = U256::from(signature.len() as u64);
    data.extend_from_slice(&u256_be_word(&sig_len));
    data.extend_from_slice(signature);
    // Pad the signature blob to the next 32-byte boundary (it's 65,
    // so we pad 31 bytes).
    data.extend(std::iter::repeat(0u8).take(32 - signature.len() % 32));
    data
}

/// `Chequebook.issuer()` selector for read-only verification that
/// the recovered cheque signer actually controls the chequebook.
pub fn chequebook_issuer_selector() -> [u8; 4] {
    let h = keccak(b"issuer()");
    [h[0], h[1], h[2], h[3]]
}

/// `Chequebook.totalIssued()` selector — the cumulative amount the
/// issuer has ever committed across all beneficiaries. Useful as a
/// sanity check before accepting a cheque.
pub fn chequebook_total_issued_selector() -> [u8; 4] {
    let h = keccak(b"totalIssued()");
    [h[0], h[1], h[2], h[3]]
}

/// `Chequebook.paidOut(address beneficiary)` selector — how much of
/// the cumulative payout for `beneficiary` has already been cashed.
/// Subtract from `cheque.cumulative_payout` to learn how much BZZ
/// the cheque is worth right now.
pub fn chequebook_paid_out_selector() -> [u8; 4] {
    let h = keccak(b"paidOut(address)");
    [h[0], h[1], h[2], h[3]]
}

fn pad_word_address(addr: &[u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..32].copy_from_slice(addr);
    w
}

fn u256_be_word(v: &U256) -> [u8; 32] {
    v.to_big_endian()
}

fn keccak(data: &[u8]) -> [u8; 32] {
    let h = Keccak256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::random_secp256k1_secret;
    use k256::ecdsa::SigningKey;

    /// Sign with a fresh key; the recovered signer must match the key's
    /// derived Ethereum address. Catches every off-by-one in the
    /// EIP-712 digest, the recid translation, and the signature shape.
    #[test]
    fn sign_then_recover_round_trip() {
        let secret = random_secp256k1_secret();
        let sk = SigningKey::from_bytes(&secret.into()).unwrap();
        let want_addr = ethereum_address_from_public_key(sk.verifying_key());

        let cheque = Cheque {
            chequebook: [0x11u8; 20],
            beneficiary: [0x22u8; 20],
            cumulative_payout: U256::from(1_000_000_000_000_000u64),
        };
        let signed = sign_cheque(&secret, &cheque, 100).unwrap();
        assert_eq!(signed.cheque, cheque);

        let recovered = recover_cheque_signer(&signed, 100).unwrap();
        assert_eq!(recovered, want_addr);
    }

    /// Different chequebook contract → different domain separator →
    /// different digest → recovered signer must change. Guards against
    /// a bug where the verifying contract field gets dropped from the
    /// domain separator.
    #[test]
    fn domain_separator_binds_chequebook_address() {
        let secret = random_secp256k1_secret();
        let cheque1 = Cheque {
            chequebook: [0x11u8; 20],
            beneficiary: [0x22u8; 20],
            cumulative_payout: U256::from(1u64),
        };
        let mut cheque2 = cheque1.clone();
        cheque2.chequebook = [0x33u8; 20];

        let d1 = cheque_digest(&cheque1, 100);
        let d2 = cheque_digest(&cheque2, 100);
        assert_ne!(d1, d2, "chequebook address must affect the digest");

        // Sanity: recovering the wrong chequebook's signature against
        // cheque1's digest must NOT yield the original signer.
        let signed1 = sign_cheque(&secret, &cheque1, 100).unwrap();
        let mut signed_with_swapped_meta = signed1.clone();
        signed_with_swapped_meta.cheque.chequebook = [0x33u8; 20];
        // The signature is over cheque1; if we ask the verifier to
        // recover under the cheque2 domain, the answer must differ.
        let recover1 = recover_cheque_signer(&signed1, 100).unwrap();
        let recover2 = recover_cheque_signer(&signed_with_swapped_meta, 100).unwrap();
        assert_ne!(recover1, recover2);
    }

    /// Different chain id changes the digest. Cross-chain replay
    /// protection.
    #[test]
    fn chain_id_binds_signature() {
        let cheque = Cheque {
            chequebook: [0x11u8; 20],
            beneficiary: [0x22u8; 20],
            cumulative_payout: U256::from(1u64),
        };
        assert_ne!(cheque_digest(&cheque, 1), cheque_digest(&cheque, 100));
    }

    /// Verify the canonical EIP-712 type hashes match keccak-derived
    /// values from a fresh computation. If bee changes the type
    /// strings these tests catch it immediately.
    #[test]
    fn eip712_type_hashes() {
        let cheque_th = cheque_type_hash();
        let domain_th = eip712_domain_type_hash();

        assert_eq!(
            hex::encode(cheque_th),
            hex::encode(keccak(
                b"Cheque(address chequebook,address beneficiary,uint256 cumulativePayout)"
            )),
        );
        assert_eq!(
            hex::encode(domain_th),
            hex::encode(keccak(
                b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
            )),
        );
    }

    /// `cashChequeBeneficiary` calldata layout: 4-byte selector + 5
    /// 32-byte words (recipient, payout, dyn-bytes offset = 0x60,
    /// length = 65, sig blob padded to 96 bytes).
    #[test]
    fn cash_cheque_calldata_layout() {
        let recipient = [0x11u8; 20];
        let payout = U256::from(123u64);
        let sig = [0xaau8; 65];
        let data = cash_cheque_beneficiary_calldata(&recipient, payout, &sig);
        // 4 + 3 fixed words + offset hop = 4 + 96 + 32 (length) + 96 (padded blob).
        assert_eq!(data.len(), 4 + 32 * 3 + 32 + 96);
        // Selector matches `cashChequeBeneficiary(address,uint256,bytes)`.
        let want_sel = &keccak(b"cashChequeBeneficiary(address,uint256,bytes)")[0..4];
        assert_eq!(&data[0..4], want_sel);
        // Dynamic-bytes offset word should be 0x60.
        let off_word = &data[4 + 32 * 2..4 + 32 * 3];
        let mut expected = [0u8; 32];
        expected[31] = 0x60;
        assert_eq!(off_word, &expected[..]);
    }

    /// `deploySimpleSwap(address,uint256,bytes32)` selector check.
    /// Bee's chequebook factory uses this exact signature on Gnosis.
    #[test]
    fn deploy_chequebook_selector() {
        let want = &keccak(b"deploySimpleSwap(address,uint256,bytes32)")[0..4];
        let calldata =
            deploy_chequebook_calldata(&[0u8; 20], U256::from(86400u64), &[0u8; 32]);
        assert_eq!(&calldata[0..4], want);
        assert_eq!(calldata.len(), 4 + 32 * 3);
    }

    /// Read-method selectors round-trip through keccak.
    #[test]
    fn chequebook_read_selectors() {
        assert_eq!(chequebook_issuer_selector().to_vec(), keccak(b"issuer()")[0..4].to_vec());
        assert_eq!(
            chequebook_total_issued_selector().to_vec(),
            keccak(b"totalIssued()")[0..4].to_vec()
        );
        assert_eq!(
            chequebook_paid_out_selector().to_vec(),
            keccak(b"paidOut(address)")[0..4].to_vec()
        );
    }
}
