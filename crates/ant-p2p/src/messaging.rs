//! Message dispatch for the GSOC/PSS lurker.
//!
//! The lurker pulls chunks out of a target neighborhood bin (see
//! [`crate::pullsync`]) and hands each one here. This module is the pure,
//! side-effect-free classifier that turns a delivered chunk into a
//! decoded application message — the same decision bee makes in its
//! pushsync/pullsync handlers, where a valid CAC goes to `pss.TryUnwrap`
//! and a valid SOC goes to the `gsoc` dispatcher.
//!
//! Two subscriptions drive it:
//!
//! - **GSOC**: a set of watched SOC addresses (`keccak256(id ‖ owner)`).
//!   A delivered SOC whose address is watched yields its inner CAC
//!   payload (bee's gsoc handler strips the 8-byte span and delivers the
//!   rest). We re-validate the SOC signature so a peer can't inject a
//!   payload under someone else's address.
//! - **PSS**: this node's PSS secret plus the registered topics. A
//!   delivered CAC is run through [`ant_crypto::pss::unwrap`]; a hit
//!   yields the decrypted message and the matching topic.
//!
//! Matching is by content, not by trusting the delivering peer: GSOC
//! addresses are recomputed from the chunk's own id+owner and PSS
//! messages must pass the topic hint + integrity check.

use ant_crypto::pss;
use ant_crypto::{keccak256, soc_valid, SOC_HEADER_SIZE, SOC_ID_SIZE, SPAN_SIZE};
use std::collections::HashSet;

/// What the lurker is currently listening for.
#[derive(Clone, Default)]
pub struct WatchState {
    /// Watched GSOC SOC addresses (`keccak256(id ‖ owner)`).
    pub gsoc_addresses: HashSet<[u8; 32]>,
    /// Registered PSS topics (`keccak256(topic_string)`).
    pub pss_topics: Vec<[u8; 32]>,
    /// This node's PSS secret, if PSS receive is enabled.
    pub pss_secret: Option<[u8; 32]>,
}

impl WatchState {
    /// Whether anything is being watched — lets the driver skip the pull
    /// loop entirely when idle.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.gsoc_addresses.is_empty() && (self.pss_topics.is_empty() || self.pss_secret.is_none())
    }
}

/// A decoded message ready to hand to a subscriber.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedMessage {
    /// A GSOC update to a watched address; `payload` is the inner CAC
    /// content (span stripped).
    Gsoc { address: [u8; 32], payload: Vec<u8> },
    /// A PSS message decrypted under one of our topics.
    Pss { topic: [u8; 32], message: Vec<u8> },
}

/// Classify one delivered chunk against the watch state. Returns the
/// decoded message if it matches a GSOC address or a PSS topic, else
/// `None` (the overwhelmingly common case for neighborhood traffic we
/// aren't the target of).
///
/// `address` is the chunk's routed address; `data` is its wire data
/// (`span(8) ‖ payload` for a CAC, or `id(32) ‖ sig(65) ‖ inner_cac` for
/// a SOC — the two forms bee's handlers distinguish).
#[must_use]
pub fn classify(address: &[u8; 32], data: &[u8], watch: &WatchState) -> Option<DecodedMessage> {
    // GSOC: a single-owner chunk whose self-bound address we watch.
    if let Some(msg) = classify_gsoc(address, data, watch) {
        return Some(msg);
    }
    // PSS: a trojan CAC addressed to one of our topics.
    classify_pss(data, watch)
}

fn classify_gsoc(address: &[u8; 32], data: &[u8], watch: &WatchState) -> Option<DecodedMessage> {
    if watch.gsoc_addresses.is_empty() || !watch.gsoc_addresses.contains(address) {
        return None;
    }
    // Re-validate the SOC self-binding: the delivered wire must actually
    // hash+recover to this address, or a peer is spoofing.
    if !soc_valid(address, data) {
        return None;
    }
    // Inner CAC = data[SOC_HEADER..]; its payload is everything past the
    // 8-byte span (bee's gsoc handler: `WrappedChunk().Data()[SpanSize:]`).
    let inner = data.get(SOC_HEADER_SIZE..)?;
    let payload = inner.get(SPAN_SIZE..)?.to_vec();
    Some(DecodedMessage::Gsoc {
        address: *address,
        payload,
    })
}

fn classify_pss(data: &[u8], watch: &WatchState) -> Option<DecodedMessage> {
    let secret = watch.pss_secret.as_ref()?;
    if watch.pss_topics.is_empty() || data.len() != pss::TROJAN_DATA_SIZE {
        return None;
    }
    let (topic, message) = pss::unwrap(secret, data, &watch.pss_topics)?;
    Some(DecodedMessage::Pss { topic, message })
}

/// Convenience: the SOC address an `(identifier, owner)` pair resolves
/// to, for callers building a GSOC watch set — `keccak256(id ‖ owner)`.
#[must_use]
pub fn gsoc_watch_address(identifier: &[u8; SOC_ID_SIZE], owner: &[u8; 20]) -> [u8; 32] {
    let mut input = [0u8; SOC_ID_SIZE + 20];
    input[..SOC_ID_SIZE].copy_from_slice(identifier);
    input[SOC_ID_SIZE..].copy_from_slice(owner);
    keccak256(&input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::gsoc::{build_gsoc_chunk, gsoc_mine};
    use ant_crypto::pss::{topic_from_string, wrap, Recipient};
    use ant_crypto::{ethereum_address_from_public_key, SECP256K1_SECRET_LEN};
    use k256::ecdsa::{SigningKey, VerifyingKey};

    fn owner_of(secret: &[u8; SECP256K1_SECRET_LEN]) -> [u8; 20] {
        let sk = SigningKey::from_bytes(secret.into()).unwrap();
        ethereum_address_from_public_key(&VerifyingKey::from(&sk))
    }

    #[test]
    fn decodes_a_watched_gsoc_update() {
        // A sender mines a key into some target neighborhood and signs an
        // update; the lurker watching that SOC address decodes it.
        let overlay = [0x42u8; 32];
        let identifier = [0x11u8; 32];
        let secret = gsoc_mine(&overlay, &identifier, 8).unwrap();
        let chunk = build_gsoc_chunk(&secret, &identifier, b"gsoc payload").unwrap();

        let watch = WatchState {
            gsoc_addresses: HashSet::from([chunk.address]),
            ..Default::default()
        };
        let got = classify(&chunk.address, &chunk.wire, &watch);
        assert_eq!(
            got,
            Some(DecodedMessage::Gsoc {
                address: chunk.address,
                payload: b"gsoc payload".to_vec(),
            })
        );

        // The watch address matches the (identifier, owner) helper.
        let owner = owner_of(&secret);
        assert_eq!(gsoc_watch_address(&identifier, &owner), chunk.address);
    }

    #[test]
    fn ignores_unwatched_gsoc_and_spoofed_address() {
        let overlay = [0x42u8; 32];
        let identifier = [0x22u8; 32];
        let secret = gsoc_mine(&overlay, &identifier, 8).unwrap();
        let chunk = build_gsoc_chunk(&secret, &identifier, b"x").unwrap();

        // Not watching this address → ignored.
        let empty = WatchState::default();
        assert!(classify(&chunk.address, &chunk.wire, &empty).is_none());

        // Watching a different address, but delivered under it → soc_valid
        // fails the self-bind, so no spoof leaks through.
        let watch = WatchState {
            gsoc_addresses: HashSet::from([[0x99u8; 32]]),
            ..Default::default()
        };
        assert!(classify(&[0x99u8; 32], &chunk.wire, &watch).is_none());
    }

    #[test]
    fn decodes_a_pss_message_for_registered_topic() {
        let topic = topic_from_string("lurker-topic");
        let secret = [0x07u8; 32];
        let sk = SigningKey::from_bytes(&secret.into()).unwrap();
        let pk = k256::PublicKey::from(&VerifyingKey::from(&sk));
        let (address, data) =
            wrap(&topic, b"secret pss", &Recipient::Key(pk), &[vec![0x00]]).unwrap();

        let watch = WatchState {
            pss_topics: vec![topic],
            pss_secret: Some(secret),
            ..Default::default()
        };
        let got = classify(&address, &data, &watch);
        assert_eq!(
            got,
            Some(DecodedMessage::Pss {
                topic,
                message: b"secret pss".to_vec(),
            })
        );

        // Wrong secret / unregistered topic → nothing.
        let other = WatchState {
            pss_topics: vec![topic_from_string("different")],
            pss_secret: Some(secret),
            ..Default::default()
        };
        assert!(classify(&address, &data, &other).is_none());
    }

    #[test]
    fn empty_watch_short_circuits() {
        assert!(WatchState::default().is_empty());
        let only_topic = WatchState {
            pss_topics: vec![[0u8; 32]],
            pss_secret: None,
            ..Default::default()
        };
        assert!(
            only_topic.is_empty(),
            "topics without a secret can't receive"
        );
    }
}
