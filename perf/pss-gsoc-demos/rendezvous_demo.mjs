// Rendezvous many-to-many demo (own-neighborhood room, bee-js subscribe).
// Multiple independent senders (Alice/Bob/Carol) publish to a shared room
// (a PSS topic broadcast into the participant's neighborhood); the
// participant receives them all — a group channel, no central server.
import { Bee, Topic } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API = 'http://127.0.0.1:1733';
const BATCH = process.env.BATCH;
const OVERLAY = process.env.OVERLAY;
const bee = new Bee(API);

const ROOM = 'swarm-room-' + Date.now();
const topic = Topic.fromString(ROOM);
const target = OVERLAY.slice(0, 4);
console.log('room:', ROOM, ' neighborhood:', OVERLAY.slice(0, 12) + '…', ' target:', target, '\n');

const received = [];
const sub = bee.pssSubscribe(topic, {
  onMessage: (msg) => {
    const s = Buffer.from(msg.toUint8Array ? msg.toUint8Array() : msg).toString('utf8');
    console.log('  📨 room delivered:', s);
    received.push(s);
  },
  onError: (e) => console.log('sub error:', e.message),
  onClose: () => {},
});

await new Promise((r) => setTimeout(r, 24000)); // reside deep

const senders = ['Alice', 'Bob', 'Carol'];
const sent = [];
for (const who of senders) {
  const text = `${who}: hi room @ ${new Date().toISOString()}`;
  sent.push(text);
  console.log('  ✉️ ', who, 'sends');
  await bee.pssSend(BATCH, topic, target, text);
  await new Promise((r) => setTimeout(r, 8000)); // space sends out
}

console.log('\nall sent; waiting up to 90s…');
const deadline = Date.now() + 90000;
while (received.length < sent.length && Date.now() < deadline) await new Promise((r) => setTimeout(r, 1000));
sub.cancel();

const got = new Set(received);
console.log(`\nreceived ${received.length}/${sent.length}`);
if (sent.every((m) => got.has(m))) { console.log('✅ RENDEZVOUS DEMO PASS — all senders delivered to the room'); process.exit(0); }
else { console.log('❌ missing:', sent.filter((m) => !got.has(m))); process.exit(2); }
