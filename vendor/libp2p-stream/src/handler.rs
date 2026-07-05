use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    channel::{mpsc, oneshot},
    StreamExt as _,
};
use libp2p_identity::PeerId;
use libp2p_swarm::{
    self as swarm,
    handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
    ConnectionHandler, Stream, StreamProtocol,
};

use crate::{shared::Shared, upgrade::Upgrade, OpenStreamError};

pub struct Handler {
    remote: PeerId,
    shared: Arc<Mutex<Shared>>,
    /// Cap on concurrent outbound substream upgrades per connection.
    /// Upstream hard-codes 1 (a singular `pending_upgrade` slot),
    /// serialising every substream open behind the previous one —
    /// on ant's pushsync workload that is one protocol negotiation
    /// RTT *per chunk per connection*. Perf-lab Experiment 3 keys
    /// the in-flight upgrades instead (hoverfly's stream_pool patch,
    /// measured 1.8× there).
    max_concurrent_upgrades: usize,

    receiver: mpsc::Receiver<NewStream>,
    /// In-flight outbound upgrades keyed by `OutboundOpenInfo` so the
    /// `FullyNegotiatedOutbound` / `DialUpgradeError` events can be
    /// correlated back to the requester's oneshot sender.
    pending_upgrades: HashMap<
        u64,
        (
            StreamProtocol,
            oneshot::Sender<Result<Stream, OpenStreamError>>,
        ),
    >,
    /// Requests pulled off the channel but not yet yielded to the
    /// swarm (one event per `poll` call; extras queue here).
    pending_emit: VecDeque<(u64, Upgrade)>,
    next_upgrade_id: u64,
}

impl Handler {
    pub(crate) fn new(
        remote: PeerId,
        shared: Arc<Mutex<Shared>>,
        receiver: mpsc::Receiver<NewStream>,
        max_concurrent_upgrades: usize,
    ) -> Self {
        Self {
            shared,
            receiver,
            max_concurrent_upgrades: max_concurrent_upgrades.max(1),
            pending_upgrades: HashMap::new(),
            pending_emit: VecDeque::new(),
            next_upgrade_id: 0,
            remote,
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_upgrade_id;
        self.next_upgrade_id = self.next_upgrade_id.wrapping_add(1);
        id
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = Upgrade;
    type OutboundProtocol = Upgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = u64;

    fn listen_protocol(&self) -> swarm::SubstreamProtocol<Self::InboundProtocol> {
        swarm::SubstreamProtocol::new(
            Upgrade {
                supported_protocols: Shared::lock(&self.shared).supported_inbound_protocols(),
            },
            (),
        )
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<swarm::ConnectionHandlerEvent<Self::OutboundProtocol, u64, Self::ToBehaviour>> {
        // First: emit any queued substream requests (one event per
        // `poll` call; the rest buffered in `pending_emit`).
        if let Some((id, upgrade)) = self.pending_emit.pop_front() {
            return Poll::Ready(swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: swarm::SubstreamProtocol::new(upgrade, id),
            });
        }

        // Then: pull from the `NewStream` channel until the total in
        // flight (pending + queued-to-emit) reaches the cap or the
        // channel runs dry.
        while self.pending_upgrades.len() + self.pending_emit.len() < self.max_concurrent_upgrades
        {
            match self.receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(new_stream)) => {
                    let id = self.alloc_id();
                    self.pending_upgrades
                        .insert(id, (new_stream.protocol.clone(), new_stream.sender));
                    let upgrade = Upgrade {
                        supported_protocols: vec![new_stream.protocol],
                    };
                    if self.pending_emit.is_empty() {
                        return Poll::Ready(
                            swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
                                protocol: swarm::SubstreamProtocol::new(upgrade, id),
                            },
                        );
                    }
                    self.pending_emit.push_back((id, upgrade));
                }
                Poll::Ready(None) => break, // Sender is gone, no more work to do.
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        libp2p_core::util::unreachable(event)
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: (stream, protocol),
                info: (),
            }) => {
                Shared::lock(&self.shared).on_inbound_stream(self.remote, stream, protocol);
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: (stream, actual_protocol),
                info: id,
            }) => {
                let Some((expected_protocol, sender)) = self.pending_upgrades.remove(&id) else {
                    debug_assert!(
                        false,
                        "Negotiated an outbound stream with no matching pending upgrade id"
                    );
                    return;
                };
                debug_assert_eq!(expected_protocol, actual_protocol);

                let _ = sender.send(Ok(stream));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, info: id }) => {
                let Some((p, sender)) = self.pending_upgrades.remove(&id) else {
                    debug_assert!(
                        false,
                        "Received a `DialUpgradeError` with no matching pending upgrade id"
                    );
                    return;
                };

                let error = match error {
                    swarm::StreamUpgradeError::Timeout => {
                        OpenStreamError::Io(io::Error::from(io::ErrorKind::TimedOut))
                    }
                    swarm::StreamUpgradeError::Apply(v) => libp2p_core::util::unreachable(v),
                    swarm::StreamUpgradeError::NegotiationFailed => {
                        OpenStreamError::UnsupportedProtocol(p)
                    }
                    swarm::StreamUpgradeError::Io(io) => OpenStreamError::Io(io),
                };

                let _ = sender.send(Err(error));
            }
            _ => {}
        }
    }
}

/// Message from a [`Control`](crate::Control) to
/// a [`ConnectionHandler`] to negotiate a new outbound stream.
#[derive(Debug)]
pub(crate) struct NewStream {
    pub(crate) protocol: StreamProtocol,
    pub(crate) sender: oneshot::Sender<Result<Stream, OpenStreamError>>,
}
