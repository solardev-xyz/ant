// TRUE multi-node rendezvous: two independent ant nodes, one shared
// room. Upgrades rendezvous_demo.mjs (one client, one node, three
// serial sends) into the real many-to-many claim:
//
//   node A (API_A) — hosts the room in its own neighborhood; local
//     subscriber via bee-js pssSubscribe.
//   node B (API_B) — a different overlay entirely; its subscriber
//     lurks A's neighborhood via the ?neighborhood= override (raw WS —
//     bee-js has no query passthrough), and its client SENDS into the
//     room through B's own /pss/send.
//
// PASS bar (honest): subscriber A must receive every message from
// both senders — that proves multi-node many-to-many send + receive.
// Subscriber B is reported but best-effort: a light node far from an
// arbitrary neighborhood only reaches ~bin 12 and reception there is
// documented as unreliable (PSS-GSOC.md).
//
// A and B MUST use different postage batches: two independent local
// issuers on one batch allocate colliding stamp indices in the room's
// bucket, and the storers would evict each other's chunks.
import { Bee, Topic } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API_A = process.env.API_A ?? 'http://127.0.0.1:1733';
const API_B = process.env.API_B ?? 'http://127.0.0.1:1734';
const BATCH_A = process.env.BATCH_A;
const BATCH_B = process.env.BATCH_B;
const OVERLAY_A = process.env.OVERLAY_A; // room neighborhood = node A's
const beeA = new Bee(API_A);
const beeB = new Bee(API_B);

const ROOM = 'swarm-room-' + Date.now();
const topic = Topic.fromString(ROOM);
const target = OVERLAY_A.slice(0, 4);
console.log('room:', ROOM, ' neighborhood(A):', OVERLAY_A.slice(0, 12) + '…', ' target:', target, '\n');

const gotA = [];
const subA = beeA.pssSubscribe(topic, {
  onMessage: (m) => {
    const s = Buffer.from(m.toUint8Array ? m.toUint8Array() : m).toString('utf8');
    console.log('  📨 [A] delivered:', s);
    gotA.push(s);
  },
  onError: (e) => console.log('[A] sub error:', e.message),
  onClose: () => {},
});

// Subscriber on node B lurking A's neighborhood (best-effort).
const gotB = [];
const wsB = new WebSocket(
  `${API_B.replace('http', 'ws')}/pss/subscribe/${topic.toString()}?neighborhood=${OVERLAY_A}`,
);
wsB.binaryType = 'arraybuffer';
wsB.onmessage = (ev) => {
  const s = Buffer.from(ev.data).toString('utf8');
  console.log('  📨 [B] delivered:', s);
  gotB.push(s);
};
wsB.onerror = () => console.log('[B] ws error');

await new Promise((r) => setTimeout(r, 24000)); // reside deep

const sent = [];
const send = async (bee, batch, who) => {
  const text = `${who}: hi room @ ${new Date().toISOString()}`;
  sent.push(text);
  console.log(`  ✉️  ${who} sends (via ${bee === beeA ? 'node A' : 'node B'})`);
  await bee.pssSend(batch, topic, target, text);
  await new Promise((r) => setTimeout(r, 8000));
};
await send(beeA, BATCH_A, 'Alice@A');
await send(beeB, BATCH_B, 'Bob@B');
await send(beeA, BATCH_A, 'Carol@A');
await send(beeB, BATCH_B, 'Dave@B');

console.log('\nall sent; waiting up to 120s…');
const deadline = Date.now() + 120000;
while (new Set(gotA).size < sent.length && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000));
}
subA.cancel();
try { wsB.close(); } catch {}

const setA = new Set(gotA);
const setB = new Set(gotB);
console.log(`\n[A] received ${setA.size}/${sent.length}; [B] received ${setB.size}/${sent.length} (best-effort)`);
if (sent.every((m) => setA.has(m))) {
  console.log('✅ MULTI-NODE RENDEZVOUS PASS — senders on two nodes, room delivered all');
  process.exit(0);
}
console.log('❌ missing at A:', sent.filter((m) => !setA.has(m)));
process.exit(2);
