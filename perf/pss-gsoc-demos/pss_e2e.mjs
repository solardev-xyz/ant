// PSS end-to-end (topic-broadcast) against a live ant node, via bee-js.
//   subscriber: pssSubscribe(topic) → WS → ant lurker (downloads CACs in
//               the node's neighborhood, unwraps via the topic-derived key)
//   sender:     pssSend(batch, topic, target=node-overlay-prefix, data)
//               → ant /pss/send → trojan chunk mined into the neighborhood
import { Bee, Topic, Utils } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API = 'http://127.0.0.1:1733';
const BATCH = process.env.BATCH;
const OVERLAY = process.env.OVERLAY;
const bee = new Bee(API);

const topic = Topic.fromString('ant-pss-e2e-' + Date.now());
const payload = 'hello from bee-js PSS @ ' + new Date().toISOString();
let received = null;

const sub = bee.pssSubscribe(topic, {
  onMessage: (msg) => {
    received = Buffer.from(msg.toUint8Array ? msg.toUint8Array() : msg).toString('utf8');
    console.log('LURKER DELIVERED:', received);
  },
  onError: (e) => console.log('sub error:', e.message),
  onClose: () => {},
});

await new Promise((r) => setTimeout(r, 22000)); // let the lurker reside

const target = Utils.makeMaxTarget(OVERLAY); // 2-byte overlay prefix
console.log('sending PSS to target', target, '(topic-broadcast, no recipient)');
await bee.pssSend(BATCH, topic, target, payload); // no recipient → broadcast
console.log('sent; waiting up to 90s…');

const deadline = Date.now() + 80000;
while (!received && Date.now() < deadline) await new Promise((r) => setTimeout(r, 1000));
sub.cancel();

if (received === payload) { console.log('\n✅ PSS E2E PASS'); process.exit(0); }
else if (received) { console.log('\n⚠️  payload mismatch:', received); process.exit(1); }
else { console.log('\n❌ PSS E2E TIMEOUT'); process.exit(2); }
