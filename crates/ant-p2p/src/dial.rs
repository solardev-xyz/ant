//! Bootstrap dial preparation: Happy-Eyeballs ordering + peer grouping.
//!
//! Given a flat list of concrete multiaddrs (post-`dnsaddr::resolve_all`),
//! group by embedded `/p2p/<peer_id>` and build one [`DialOpts`] per peer with
//! IPv6 addresses sorted first and `override_dial_concurrency_factor` tuned to
//! the address count. Addresses without a `/p2p/` suffix are discarded: Swarm
//! bootnode TXT records always include the peer id, and without it we can't
//! match returning `ConnectionEstablished` events back to a handshake target.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU8;

use libp2p::multiaddr::Protocol;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{Multiaddr, PeerId};

/// For UI: first IP literal or DNS label in a multiaddr.
pub fn endpoint_host(ma: &Multiaddr) -> String {
    for p in ma.iter() {
        match p {
            Protocol::Ip4(ip) => return ip.to_string(),
            Protocol::Ip6(ip) => return ip.to_string(),
            Protocol::Dns4(d) | Protocol::Dns6(d) | Protocol::Dnsaddr(d) => {
                return d.to_string();
            }
            _ => {}
        }
    }
    String::new()
}

/// One multiaddr per peer (first seen) for hint / bootstrap dials.
pub fn first_multiaddr_per_peer(addrs: &[Multiaddr]) -> HashMap<PeerId, Multiaddr> {
    let mut m = HashMap::new();
    for a in addrs {
        if let Some(pid) = extract_peer_id(a) {
            m.entry(pid).or_insert_with(|| a.clone());
        }
    }
    m
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpVersion {
    V4,
    V6,
}

impl IpVersion {
    fn from_multiaddr(addr: &Multiaddr) -> Option<Self> {
        addr.iter().find_map(|p| match p {
            Protocol::Ip4(_) | Protocol::Dns4(_) => Some(Self::V4),
            Protocol::Ip6(_) | Protocol::Dns6(_) => Some(Self::V6),
            _ => None,
        })
    }
}

/// Extract the `/p2p/<peer_id>` component from a multiaddr, if present.
pub fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        Protocol::P2p(id) => Some(id),
        _ => None,
    })
}

/// Build one [`DialOpts`] per distinct `PeerId`, IPv6-first, with a
/// concurrency factor derived from the address count.
pub fn bootstrap_dial_opts<I>(addrs: I) -> Vec<DialOpts>
where
    I: IntoIterator<Item = Multiaddr>,
{
    let mut by_peer: BTreeMap<PeerId, Vec<Multiaddr>> = BTreeMap::new();
    for addr in addrs {
        if let Some(pid) = extract_peer_id(&addr) {
            by_peer.entry(pid).or_default().push(addr);
        }
    }

    by_peer
        .into_iter()
        .filter_map(|(peer_id, addrs)| {
            let (sorted, concurrency) = sort_v6_first(addrs)?;
            Some(
                DialOpts::peer_id(peer_id)
                    .addresses(sorted)
                    .override_dial_concurrency_factor(concurrency)
                    .build(),
            )
        })
        .collect()
}

/// Sort addresses IPv6-first and compute a dial concurrency factor:
/// up to 8 for v6, clamped to `[1, 4]` for v4, 1 otherwise.
fn sort_v6_first(addrs: Vec<Multiaddr>) -> Option<(Vec<Multiaddr>, NonZeroU8)> {
    let mut v6 = Vec::new();
    let mut v4 = Vec::new();
    let mut other = Vec::new();
    for a in addrs {
        match IpVersion::from_multiaddr(&a) {
            Some(IpVersion::V6) => v6.push(a),
            Some(IpVersion::V4) => v4.push(a),
            None => other.push(a),
        }
    }

    let ipv6_count = v6.len();
    let ipv4_count = v4.len();
    let sorted: Vec<Multiaddr> = v6.into_iter().chain(v4).chain(other).collect();
    if sorted.is_empty() {
        return None;
    }

    let factor = if ipv6_count > 0 {
        ipv6_count.min(8) as u8
    } else if ipv4_count > 0 {
        ipv4_count.clamp(1, 4) as u8
    } else {
        1
    };
    Some((sorted, NonZeroU8::new(factor).unwrap_or(NonZeroU8::MIN)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_by_peer_and_orders_v6_first() {
        let a4: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8"
                .parse()
                .unwrap();
        let a6: Multiaddr =
            "/ip6/2001:db8::1/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8"
                .parse()
                .unwrap();
        let b4: Multiaddr =
            "/ip4/5.6.7.8/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let opts = bootstrap_dial_opts([a4, a6, b4]);
        assert_eq!(opts.len(), 2);
    }

    #[test]
    fn drops_addresses_without_p2p() {
        let a: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        assert!(bootstrap_dial_opts([a]).is_empty());
    }

    #[test]
    fn concurrency_factor_scales_with_address_count() {
        let addrs: Vec<Multiaddr> = (0..6)
            .map(|i| {
                format!("/ip6/2001:db8::{i}/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8")
                    .parse()
                    .unwrap()
            })
            .collect();
        let opts = bootstrap_dial_opts(addrs);
        assert_eq!(opts.len(), 1);
    }
}
