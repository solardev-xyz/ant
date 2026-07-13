// SYMMETRIC many-to-many rendezvous: TWO reliable subscribers on TWO
// independent nodes, both co-resident in the room's neighborhood, with
// senders on both nodes. This is the true many-to-many proof the
// many-to-one multi_node_rendezvous.mjs doesn't reach.
//
// Setup (done outside this script): both node overlays are mined into
// the same neighborhood (prefix `target`), so each node natively
// resides there and reliably receives a room broadcast to that prefix —
// no ?neighborhood= override, both subscribe to their OWN neighborhood.
//
// PASS requires BOTH subscribers to receive EVERY message, sent from
// both nodes — symmetric many-to-many, enforced on both sides.
import { Bee, Topic } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API_A = process.env.API_A ?? 'http://127.0.0.1:1733';
const API_B = process.env.API_B ?? 'http://127.0.0.1:1734';
const BATCH_A = process.env.BATCH_A;
const BATCH_B = process.env.BATCH_B;
// The shared neighborhood prefix both nodes are mined into (hex, no 0x).
const TARGET = process.env.TARGET;
const beeA = new Bee(API_A);
const beeB = new Bee(API_B);

const ROOM = 'symmetric-room-' + Date.now();
const topic = Topic.fromString(ROOM);
const target = TARGET.slice(0, 4); // 2-byte neighborhood prefix
console.log('room:', ROOM, ' shared neighborhood target:', target, '\n');

const gotA = [];
const gotB = [];
// Both subscribe to their OWN neighborhood (no override): each node is
// resident in the shared `target` neighborhood, so each covers the room.
const subA = beeA.pssSubscribe(topic, {
  onMessage: (m) => {
    const s = Buffer.from(m.toUint8Array ? m.toUint8Array() : m).toString('utf8');
    console.log('  📨 [A] delivered:', s);
    gotA.push(s);
  },
  onError: (e) => console.log('[A] sub error:', e.message),
  onClose: () => {},
});
const subB = beeB.pssSubscribe(topic, {
  onMessage: (m) => {
    const s = Buffer.from(m.toUint8Array ? m.toUint8Array() : m).toString('utf8');
    console.log('  📨 [B] delivered:', s);
    gotB.push(s);
  },
  onError: (e) => console.log('[B] sub error:', e.message),
  onClose: () => {},
});

await new Promise((r) => setTimeout(r, 30000)); // both reside deep

const sent = [];
const send = async (bee, batch, who) => {
  const text = `${who}: hi room @ ${new Date().toISOString()}`;
  sent.push(text);
  console.log(`  ✉️  ${who} sends (via ${bee === beeA ? 'node A' : 'node B'})`);
  await bee.pssSend(batch, topic, target, text);
  await new Promise((r) => setTimeout(r, 9000));
};
await send(beeA, BATCH_A, 'Alice@A');
await send(beeB, BATCH_B, 'Bob@B');
await send(beeA, BATCH_A, 'Carol@A');
await send(beeB, BATCH_B, 'Dave@B');

console.log('\nall sent; waiting up to 150s for BOTH subscribers…');
const deadline = Date.now() + 150000;
const bothDone = () =>
  sent.every((m) => new Set(gotA).has(m)) && sent.every((m) => new Set(gotB).has(m));
while (!bothDone() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 1000));
subA.cancel();
subB.cancel();

const setA = new Set(gotA);
const setB = new Set(gotB);
console.log(`\n[A] received ${setA.size}/${sent.length}; [B] received ${setB.size}/${sent.length}`);
const aAll = sent.every((m) => setA.has(m));
const bAll = sent.every((m) => setB.has(m));
if (aAll && bAll) {
  console.log('✅ SYMMETRIC MANY-TO-MANY PASS — two reliable subscribers on two nodes both got every message from both senders');
  process.exit(0);
}
if (!aAll) console.log('❌ missing at A:', sent.filter((m) => !setA.has(m)));
if (!bAll) console.log('❌ missing at B:', sent.filter((m) => !setB.has(m)));
process.exit(2);
